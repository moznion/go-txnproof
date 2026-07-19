package txnproof

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// StatementRecord is one SQL statement observed inside a boundary.
type StatementRecord struct {
	Query string
	Kind  StatementKind
	// TxID identifies the driver-level transaction the statement ran in.
	// 0 means the statement ran in auto-commit mode. IDs are process-local
	// sequence numbers, not database transaction IDs.
	TxID uint64
	Time time.Time
	// Attrs is populated only on records delivered to
	// UnboundedWriteReporter, with the result of WithBoundaryAttrsFunc
	// evaluated against the statement's context at record time. Records in
	// Violation.Statements leave it nil — the boundary's attrs live on the
	// Violation itself.
	Attrs []BoundaryAttr
}

// Violation is reported when a boundary's write statements span two or more
// atomic units (distinct transactions and/or auto-commit statements), meaning
// the boundary is not atomic: a crash between units leaves partial state.
type Violation struct {
	// Boundary is the name given to StartBoundary.
	Boundary string
	// WriteUnits is the number of distinct atomic units that contained
	// writes. Atomic execution means WriteUnits == 1.
	WriteUnits int
	// Statements is the recorded statement timeline of the boundary
	// (reads included), capped by WithMaxRecordedStatements.
	Statements []StatementRecord
	// TruncatedStatements is how many statements were dropped from
	// Statements due to the cap.
	TruncatedStatements int
	// Attrs is the contextual metadata attached to the boundary (trace ID,
	// request ID, ...): the result of WithBoundaryAttrsFunc evaluated at
	// boundary start, followed by any WithBoundaryAttrs entries. Duplicate
	// keys are kept in order.
	Attrs []BoundaryAttr
}

// String renders a human-readable multi-line summary listing the write
// statements grouped by atomic unit.
func (v Violation) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "txnproof: boundary %q is not atomic: writes span %d atomic units:", v.Boundary, v.WriteUnits)
	for _, s := range v.Statements {
		if s.Kind != KindWrite {
			continue
		}
		unit := "auto-commit"
		if s.TxID != 0 {
			unit = fmt.Sprintf("tx#%d", s.TxID)
		}
		fmt.Fprintf(&sb, "\n  [%s] %s", unit, strings.Join(strings.Fields(s.Query), " "))
	}
	if v.TruncatedStatements > 0 {
		fmt.Fprintf(&sb, "\n  (+%d statements truncated)", v.TruncatedStatements)
	}
	return sb.String()
}

// Reporter receives detected violations. Implementations decide what to do:
// fail a test, log, emit a metric, notify an error tracker.
type Reporter interface {
	Report(ctx context.Context, v Violation)
}

// ReporterFunc adapts a function to the Reporter interface.
type ReporterFunc func(ctx context.Context, v Violation)

func (f ReporterFunc) Report(ctx context.Context, v Violation) { f(ctx, v) }

// UnboundedWriteReporter is an optional extension a Reporter can implement to
// also receive write statements executed with no boundary in their context
// (requires WithUnboundedWriteDetection).
type UnboundedWriteReporter interface {
	ReportUnboundedWrite(ctx context.Context, s StatementRecord)
}

// NestedBoundary is reported when a boundary is started on a context that
// already carries one (requires WithNestedBoundaryDetection). The shadow
// semantics are unchanged — statements attribute to the inner boundary only —
// so a nesting occurrence is not a Violation but a coverage signal: it
// usually means two instrumentation layers overlap (e.g. a resolver
// middleware and a use-case middleware both start boundaries).
type NestedBoundary struct {
	// Outer is the name of the boundary that was already on the context.
	Outer string
	// Inner is the name of the newly started, shadowing boundary.
	Inner string
	// Time is when the inner boundary was started.
	Time time.Time
}

// NestedBoundaryReporter is an optional extension a Reporter can implement to
// also receive nested-boundary occurrences (requires
// WithNestedBoundaryDetection).
type NestedBoundaryReporter interface {
	ReportNestedBoundary(ctx context.Context, n NestedBoundary)
}

// StaleAllow is reported when a boundary marked with AllowNonAtomic finishes
// without a violation to suppress: the allow did nothing for this execution.
// Note that this is per execution — a boundary whose write count varies by
// code path can legitimately produce both violations-suppressed and
// StaleAllow reports.
type StaleAllow struct {
	// Boundary is the name given to StartBoundary.
	Boundary string
	// Reason is the reason given to AllowNonAtomic.
	Reason string
	// WriteUnits is the number of atomic units the boundary actually used
	// (0 or 1).
	WriteUnits int
}

// StaleAllowReporter is an optional extension a Reporter can implement to
// receive stale AllowNonAtomic marks (see StaleAllow).
type StaleAllowReporter interface {
	ReportStaleAllow(ctx context.Context, s StaleAllow)
}

// TestingT is the subset of *testing.T that txnproof's test helpers need.
type TestingT interface {
	Helper()
	Errorf(format string, args ...any)
}

// CollectingReporter accumulates violations in memory. Intended for tests.
type CollectingReporter struct {
	mu          sync.Mutex
	violations  []Violation
	unbounded   []StatementRecord
	staleAllows []StaleAllow
	nested      []NestedBoundary
}

var (
	_ Reporter               = (*CollectingReporter)(nil)
	_ UnboundedWriteReporter = (*CollectingReporter)(nil)
	_ StaleAllowReporter     = (*CollectingReporter)(nil)
	_ NestedBoundaryReporter = (*CollectingReporter)(nil)
)

// NewCollectingReporter creates an empty CollectingReporter.
func NewCollectingReporter() *CollectingReporter { return &CollectingReporter{} }

func (r *CollectingReporter) Report(_ context.Context, v Violation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.violations = append(r.violations, v)
}

func (r *CollectingReporter) ReportUnboundedWrite(_ context.Context, s StatementRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unbounded = append(r.unbounded, s)
}

func (r *CollectingReporter) ReportStaleAllow(_ context.Context, s StaleAllow) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.staleAllows = append(r.staleAllows, s)
}

func (r *CollectingReporter) ReportNestedBoundary(_ context.Context, n NestedBoundary) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nested = append(r.nested, n)
}

// Violations returns a copy of the collected violations.
func (r *CollectingReporter) Violations() []Violation {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Violation, len(r.violations))
	copy(out, r.violations)
	return out
}

// UnboundedWrites returns a copy of the collected unbounded write statements.
func (r *CollectingReporter) UnboundedWrites() []StatementRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]StatementRecord, len(r.unbounded))
	copy(out, r.unbounded)
	return out
}

// StaleAllows returns a copy of the collected stale AllowNonAtomic reports.
func (r *CollectingReporter) StaleAllows() []StaleAllow {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]StaleAllow, len(r.staleAllows))
	copy(out, r.staleAllows)
	return out
}

// NestedBoundaries returns a copy of the collected nested-boundary
// occurrences.
func (r *CollectingReporter) NestedBoundaries() []NestedBoundary {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]NestedBoundary, len(r.nested))
	copy(out, r.nested)
	return out
}

// Reset clears everything collected so far.
func (r *CollectingReporter) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.violations = nil
	r.unbounded = nil
	r.staleAllows = nil
	r.nested = nil
}

// RequireNoViolations fails the test with one error per collected violation.
func (r *CollectingReporter) RequireNoViolations(t TestingT) {
	t.Helper()
	for _, v := range r.Violations() {
		t.Errorf("%s", v.String())
	}
}

// RequireNoUnboundedWrites fails the test with one error per collected
// unbounded write, enforcing that every write in the exercised code ran with
// a boundary in its context (requires WithUnboundedWriteDetection).
func (r *CollectingReporter) RequireNoUnboundedWrites(t TestingT) {
	t.Helper()
	for _, s := range r.UnboundedWrites() {
		t.Errorf("txnproof: write executed with no boundary in its context: %s", s.Query)
	}
}

// RequireNoNestedBoundaries fails the test with one error per collected
// nested-boundary occurrence, enforcing that instrumentation layers do not
// overlap (requires WithNestedBoundaryDetection).
func (r *CollectingReporter) RequireNoNestedBoundaries(t TestingT) {
	t.Helper()
	for _, n := range r.NestedBoundaries() {
		t.Errorf("txnproof: boundary %q started inside boundary %q; statements attribute to the inner one only", n.Inner, n.Outer)
	}
}

// RequireNoStaleAllows fails the test with one error per stale AllowNonAtomic
// mark, keeping in-code allows subject to the same rot discipline as
// Allowlist.UnusedEntries.
func (r *CollectingReporter) RequireNoStaleAllows(t TestingT) {
	t.Helper()
	for _, s := range r.StaleAllows() {
		t.Errorf("txnproof: boundary %q is marked AllowNonAtomic (%s) but used only %d write unit(s); remove the stale allow", s.Boundary, s.Reason, s.WriteUnits)
	}
}

// SlogReporter reports violations through a *slog.Logger. Intended for
// production monitoring.
type SlogReporter struct {
	Logger *slog.Logger
}

var (
	_ Reporter               = (*SlogReporter)(nil)
	_ UnboundedWriteReporter = (*SlogReporter)(nil)
	_ StaleAllowReporter     = (*SlogReporter)(nil)
	_ NestedBoundaryReporter = (*SlogReporter)(nil)
)

// NewSlogReporter creates a SlogReporter. A nil logger means slog.Default().
func NewSlogReporter(l *slog.Logger) *SlogReporter {
	if l == nil {
		l = slog.Default()
	}
	return &SlogReporter{Logger: l}
}

func (r *SlogReporter) Report(ctx context.Context, v Violation) {
	writes := make([]string, 0, len(v.Statements))
	for _, s := range v.Statements {
		if s.Kind == KindWrite {
			unit := "auto-commit"
			if s.TxID != 0 {
				unit = fmt.Sprintf("tx#%d", s.TxID)
			}
			writes = append(writes, fmt.Sprintf("[%s] %s", unit, strings.Join(strings.Fields(s.Query), " ")))
		}
	}
	args := []any{
		slog.String("boundary", v.Boundary),
		slog.Int("write_units", v.WriteUnits),
		slog.Any("writes", writes),
	}
	for _, a := range SlogAttrs(v.Attrs) {
		args = append(args, a)
	}
	r.Logger.ErrorContext(ctx, "txnproof: non-atomic SQL execution detected", args...)
}

func (r *SlogReporter) ReportUnboundedWrite(ctx context.Context, s StatementRecord) {
	args := []any{
		slog.String("query", strings.Join(strings.Fields(s.Query), " ")),
	}
	for _, a := range SlogAttrs(s.Attrs) {
		args = append(args, a)
	}
	r.Logger.WarnContext(ctx, "txnproof: write statement executed outside any boundary", args...)
}

func (r *SlogReporter) ReportNestedBoundary(ctx context.Context, n NestedBoundary) {
	r.Logger.WarnContext(ctx, "txnproof: boundary started inside another boundary; statements attribute to the inner one only",
		slog.String("outer", n.Outer),
		slog.String("inner", n.Inner),
	)
}

func (r *SlogReporter) ReportStaleAllow(ctx context.Context, s StaleAllow) {
	r.Logger.WarnContext(ctx, "txnproof: boundary is marked AllowNonAtomic but did not violate",
		slog.String("boundary", s.Boundary),
		slog.String("reason", s.Reason),
		slog.Int("write_units", s.WriteUnits),
	)
}
