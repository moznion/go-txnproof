// Package txnproof detects non-atomic SQL execution: multiple write statements
// that run inside one logical boundary (a use case, a request, a job) without
// being wrapped in a single database transaction.
//
// It works as a database/sql driver middleware, so the same detector serves
// three modes: pure unit tests (via NewNullDB), tests against a real database,
// and continuous production monitoring (via pluggable Reporters).
package txnproof

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Detector is the core of txnproof. Wrap a driver (or connector) with it, mark
// logical boundaries with StartBoundary / InBoundary, and it reports a
// Violation whenever a boundary executes two or more write statements that do
// not share a single transaction.
type Detector struct {
	reporters             []Reporter
	allowlist             *Allowlist
	classify              Classifier
	reportUnbounded       bool
	reportNested          bool
	maxRecordedStatements int
	attrsFunc             func(ctx context.Context) []BoundaryAttr

	txSeq atomic.Uint64
}

// Option configures a Detector.
type Option func(*Detector)

// WithReporter appends reporters that receive detected violations.
func WithReporter(rs ...Reporter) Option {
	return func(d *Detector) { d.reporters = append(d.reporters, rs...) }
}

// WithAllowlist installs an allowlist of boundary names whose violations are
// intentionally suppressed.
func WithAllowlist(a *Allowlist) Option {
	return func(d *Detector) { d.allowlist = a }
}

// WithClassifier replaces DefaultClassifier for statement classification.
// The classifier must be a pure function of the query text: for statements
// executed through a prepared statement it is evaluated once at Prepare and
// the result is reused for every execution.
func WithClassifier(c Classifier) Option {
	return func(d *Detector) { d.classify = c }
}

// WithUnboundedWriteDetection makes the detector notify reporters that
// implement UnboundedWriteReporter about write statements executed with no
// boundary in their context (e.g. writes from detached goroutines).
func WithUnboundedWriteDetection() Option {
	return func(d *Detector) { d.reportUnbounded = true }
}

// WithNestedBoundaryDetection makes the detector notify reporters that
// implement NestedBoundaryReporter whenever a boundary is started on a
// context that already carries one. The shadow semantics are unchanged —
// statements still attribute to the inner boundary only — this option merely
// makes the nesting itself observable, so accidental double instrumentation
// (e.g. middleware at two layers) does not go unnoticed.
func WithNestedBoundaryDetection() Option {
	return func(d *Detector) { d.reportNested = true }
}

// WithMaxRecordedStatements caps how many statements are kept per boundary for
// violation reports (write-unit counting itself is never truncated).
// The default is 200.
func WithMaxRecordedStatements(n int) Option {
	return func(d *Detector) { d.maxRecordedStatements = n }
}

// New creates a Detector.
func New(opts ...Option) *Detector {
	d := &Detector{
		classify:              DefaultClassifier,
		maxRecordedStatements: 200,
	}
	for _, o := range opts {
		o(d)
	}
	return d
}

func (d *Detector) nextTxID() uint64 { return d.txSeq.Add(1) }

type boundaryCtxKey struct{}

// Boundary is a live logical boundary (a use case, a request, a job): it
// accumulates the statement timeline of one execution and is finished by
// calling Finish. StartBoundary returns it both as the context to propagate
// and as the handle to finish.
//
// It implements context.Context itself so that StartBoundary can return the
// boundary directly as the context node instead of wrapping it in a separate
// context.WithValue allocation: the boundary doubles as its own value carrier,
// exactly as the standard library's *valueCtx stores its parent. parent is the
// context the boundary was started on.
type Boundary struct {
	det    *Detector
	parent context.Context

	name  string
	attrs []BoundaryAttr // immutable after StartBoundary returns

	mu sync.Mutex
	// allowed, allowReason and allowUnits hold the in-code allow mark. The
	// AllowNonAtomic option sets them before the boundary is shared, but
	// AllowNonAtomicHere may set them again on a live boundary, which is why
	// they live under the lock and are read there at Finish. allowUnits are the
	// exact write-unit counts the allow covers; empty means unconstrained (any
	// violating count is suppressed).
	allowed     bool
	allowReason string
	allowUnits  []int
	statements  []StatementRecord
	// Distinct transactions that contained >=1 write. The first
	// inlineWriteTxCap IDs live inline in writeTxInline (writeTxN counts how
	// many distinct IDs have been seen in total), so a boundary spanning only a
	// few transactions tallies its write-units without allocating a map at all;
	// IDs past the inline capacity spill into writeTxOverflow.
	writeTxInline    [inlineWriteTxCap]uint64
	writeTxN         int
	writeTxOverflow  map[uint64]struct{}
	autoCommitWrites int // each auto-commit write is its own atomic unit
	truncated        int
	finished         bool
}

// inlineWriteTxCap is how many distinct write-transaction IDs a boundary can
// track before spilling to a map. Boundaries almost always span a single (or a
// small handful of) transaction, so a modest inline capacity keeps the common
// path allocation-free without bloating the struct.
const inlineWriteTxCap = 4

// initialStatementCap is the capacity given to the statement-record buffer on
// its first append, chosen to absorb the typical handful of statements in one
// allocation instead of several slice regrowths (bounded by the per-boundary
// recording cap).
const initialStatementCap = 8

var _ context.Context = (*Boundary)(nil)

// noteWriteTx records that txID contained a write, counting each distinct
// transaction ID once. It must be called with b.mu held.
func (b *Boundary) noteWriteTx(txID uint64) {
	inline := b.writeTxN
	if inline > inlineWriteTxCap {
		inline = inlineWriteTxCap
	}
	for i := 0; i < inline; i++ {
		if b.writeTxInline[i] == txID {
			return
		}
	}
	if b.writeTxOverflow != nil {
		if _, ok := b.writeTxOverflow[txID]; ok {
			return
		}
	}
	if b.writeTxN < inlineWriteTxCap {
		b.writeTxInline[b.writeTxN] = txID
	} else {
		if b.writeTxOverflow == nil {
			b.writeTxOverflow = make(map[uint64]struct{})
		}
		b.writeTxOverflow[txID] = struct{}{}
	}
	b.writeTxN++
}

func (b *Boundary) Deadline() (time.Time, bool) { return b.parent.Deadline() }
func (b *Boundary) Done() <-chan struct{}       { return b.parent.Done() }
func (b *Boundary) Err() error                  { return b.parent.Err() }

func (b *Boundary) Value(key any) any {
	if key == (boundaryCtxKey{}) {
		return b
	}
	return b.parent.Value(key)
}

// Finish evaluates the boundary and reports a Violation if its writes span two
// or more atomic units. Call it exactly when the boundary ends (typically via
// defer); it is idempotent.
func (b *Boundary) Finish() { b.det.finishBoundary(b) }

// BoundaryOption configures a single boundary at StartBoundary / InBoundary.
type BoundaryOption func(*Boundary)

// AllowNonAtomic marks the boundary as intentionally non-atomic, suppressing
// its Violation at the call site — the in-code alternative to a central
// Allowlist entry. The reason should say why and reference a ticket.
//
// The optional exactWriteUnits pin how much non-atomicity the mark covers: the
// allow then applies only when the boundary finishes with exactly one of the
// given write-unit counts, and any other count is reported as a Violation as
// if the boundary carried no mark at all (the central Allowlist is still
// consulted afterwards). It keeps a reviewed exemption from silently growing
// as the boundary accumulates writes:
//
//	// exactly the domain write plus the audit write, nothing more
//	txnproof.AllowNonAtomic("audit writes are best-effort (TICKET-123)", 2)
//
// Passing several counts allows each of them, for a boundary whose write count
// legitimately differs per code path. A write unit is one transaction that
// contained at least one write, or one auto-commit write — the same number
// reported as Violation.WriteUnits, not the number of transactions — so counts
// below 2 can never match a violation and make the mark permanently stale.
//
// Rot prevention works per execution instead of per entry: when an allowed
// boundary finishes with fewer than 2 write units (i.e. the allow suppressed
// nothing), reporters that implement StaleAllowReporter are notified — the
// same discipline as unused //nolint directives. A count the mark does not
// cover needs no such signal: it surfaces as the Violation itself.
//
// AllowNonAtomicHere marks the same thing from the site of the write instead
// of from the boundary start.
func AllowNonAtomic(reason string, exactWriteUnits ...int) BoundaryOption {
	// Copied once here, not per boundary: the returned option may be reused for
	// several boundaries, and the caller's slice must not be able to change
	// what an already started boundary allows.
	units := append([]int(nil), exactWriteUnits...)
	return func(b *Boundary) {
		// Options run before StartBoundary returns, i.e. before the boundary
		// can reach another goroutine, so the mark needs no lock here (unlike
		// AllowNonAtomicHere, which marks a live boundary).
		b.allowed = true
		b.allowReason = reason
		b.allowUnits = units
	}
}

// AllowNonAtomicHere marks the boundary in ctx as intentionally non-atomic,
// suppressing its Violation exactly like the AllowNonAtomic boundary option:
// the two differ only in where the exemption lives, never in what it can
// express. The reason and the optional exactWriteUnits mean the same thing
// there, down to falling through to the central Allowlist when the boundary
// finishes with a count the mark does not cover.
//
// It exists because a boundary is usually started far away from the code that
// makes it non-atomic — in a middleware or a use-case entry point, while the
// reason for the extra write is at the extra write. Marking it there keeps the
// explanation next to the code it explains, and keeps that explanation running
// code rather than a comment:
//
//	// The audit row is written outside the domain transaction on purpose, so a
//	// failing audit sink cannot roll back the business change (TICKET-123).
//	txnproof.AllowNonAtomicHere(ctx, "audit write is best-effort (TICKET-123)", 2)
//	_, err := db.ExecContext(ctx, "INSERT INTO audit ...")
//
// The mark applies to the innermost boundary in ctx, and it does not matter
// when during the boundary's life it is called: the evaluation happens at
// Finish. The last mark wins, replacing any earlier one (including one made by
// the AllowNonAtomic option). Calling it with no boundary in ctx, or after the
// boundary has finished, does nothing — the same way statements executed
// outside any boundary are ignored. Missing boundary plumbing is caught by
// WithUnboundedWriteDetection, which reports the write this call precedes.
//
// Rot prevention is unchanged: an allowed boundary that finishes with fewer
// than 2 write units notifies StaleAllowReporter. Marking at the write site
// tends to be the more durable of the two, since a mark on a conditional path
// exists only on the executions that reach it.
func AllowNonAtomicHere(ctx context.Context, reason string, exactWriteUnits ...int) {
	b, _ := ctx.Value(boundaryCtxKey{}).(*Boundary)
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.finished {
		return
	}
	b.allowed = true
	b.allowReason = reason
	// Copied for the same reason as in AllowNonAtomic: the caller keeps the
	// backing array when the counts are passed as a slice.
	b.allowUnits = append([]int(nil), exactWriteUnits...)
}

// coversWriteUnits reports whether an allow constrained to the given exact
// write-unit counts applies to a boundary that finished with units of them. No
// counts means unconstrained: every violating count is covered.
//
// Both allow mechanisms share it so that an in-code AllowNonAtomic mark and an
// Allowlist entry with the same counts decide identically.
func coversWriteUnits(allowed []int, units int) bool {
	if len(allowed) == 0 {
		return true
	}
	for _, n := range allowed {
		if n == units {
			return true
		}
	}
	return false
}

// StartBoundary marks the beginning of a logical boundary (a use case, a
// request handler, a job) on the context. Every statement executed through a
// wrapped driver with the returned context is attributed to this boundary.
//
// It returns the boundary both as the context to propagate and as the *Boundary
// handle to finish. Call Boundary.Finish exactly when the boundary ends
// (typically via defer) to evaluate it and report a Violation if its writes
// span two or more atomic units; Finish is idempotent.
//
// Starting a boundary on a context that already carries one shadows the outer
// boundary for statements executed with the new context (reported when
// WithNestedBoundaryDetection is on).
func (d *Detector) StartBoundary(ctx context.Context, name string, opts ...BoundaryOption) (context.Context, *Boundary) {
	// The option check comes first: the outer-boundary lookup walks the whole
	// context chain, and every boundary start would pay it otherwise.
	if d.reportNested {
		if outer, _ := ctx.Value(boundaryCtxKey{}).(*Boundary); outer != nil {
			n := NestedBoundary{Outer: outer.name, Inner: name, Time: time.Now()}
			for _, r := range d.reporters {
				if nr, ok := r.(NestedBoundaryReporter); ok {
					nr.ReportNestedBoundary(ctx, n)
				}
			}
		}
	}
	// writeTxUnits is allocated lazily on the first in-transaction write, so a
	// boundary that only issues auto-commit statements (or none) never pays for
	// the map. The boundary is its own context node (see the type doc), so no
	// separate context.WithValue allocation is made here.
	b := &Boundary{det: d, parent: ctx, name: name}
	if d.attrsFunc != nil {
		// Evaluated once per boundary, before per-boundary options so that
		// WithBoundaryAttrs entries come after detector-level ones.
		b.attrs = append(b.attrs, d.attrsFunc(ctx)...)
	}
	for _, o := range opts {
		o(b)
	}
	return b, b
}

// InBoundary runs f inside a boundary and finishes it when f returns.
func (d *Detector) InBoundary(ctx context.Context, name string, f func(context.Context) error, opts ...BoundaryOption) error {
	ctx, b := d.StartBoundary(ctx, name, opts...)
	defer b.Finish()
	return f(ctx)
}

// record attributes one executed statement to the boundary in ctx (if any).
func (d *Detector) record(ctx context.Context, query string, kind StatementKind, txID uint64) {
	b, _ := ctx.Value(boundaryCtxKey{}).(*Boundary)
	if b == nil {
		if kind == KindWrite && d.reportUnbounded {
			rec := StatementRecord{Query: query, Kind: kind, TxID: txID, Time: time.Now()}
			if d.attrsFunc != nil {
				rec.Attrs = d.attrsFunc(ctx)
			}
			for _, r := range d.reporters {
				if ur, ok := r.(UnboundedWriteReporter); ok {
					ur.ReportUnboundedWrite(ctx, rec)
				}
			}
		}
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.finished {
		return
	}
	if len(b.statements) < d.maxRecordedStatements {
		if b.statements == nil {
			// Size the buffer once for the typical handful of statements
			// instead of regrowing it a few times; stay under the cap.
			c := initialStatementCap
			if c > d.maxRecordedStatements {
				c = d.maxRecordedStatements
			}
			b.statements = make([]StatementRecord, 0, c)
		}
		b.statements = append(b.statements, StatementRecord{Query: query, Kind: kind, TxID: txID, Time: time.Now()})
	} else {
		b.truncated++
	}
	if kind == KindWrite {
		if txID != 0 {
			b.noteWriteTx(txID)
		} else {
			b.autoCommitWrites++
		}
	}
}

func (d *Detector) finishBoundary(b *Boundary) {
	b.mu.Lock()
	if b.finished {
		b.mu.Unlock()
		return
	}
	b.finished = true
	units := b.writeTxN + b.autoCommitWrites
	// Snapshot the allow mark here: AllowNonAtomicHere can set it from any
	// goroutine up to this point, and setting finished above is what makes it
	// stop changing.
	allowed, allowReason, allowUnits := b.allowed, b.allowReason, b.allowUnits
	b.mu.Unlock()

	// After finished is set under the lock, record returns early without ever
	// touching b.statements / b.truncated again, so both are immutable from here
	// on and can be read without the lock. Deferring the statement snapshot to
	// the violation path below means the healthy, non-violating majority of
	// boundaries never pay for the make+copy at all.

	if units < 2 {
		if allowed {
			sa := StaleAllow{Boundary: b.name, Reason: allowReason, WriteUnits: units}
			for _, r := range d.reporters {
				if sr, ok := r.(StaleAllowReporter); ok {
					sr.ReportStaleAllow(b, sa)
				}
			}
		}
		return
	}
	// An allow constrained to exact write-unit counts falls through when the
	// boundary finished with a count it does not cover: the violation is
	// unreviewed, so it is reported like any other (the Allowlist below still
	// gets its say). The declined counts travel with the Violation so the
	// report can say that a mark exists and why it did not apply.
	var declinedUnits []int
	if allowed {
		if coversWriteUnits(allowUnits, units) {
			return
		}
		declinedUnits = allowUnits
	}
	if d.allowlist != nil {
		allowed, declined := d.allowlist.allow(b.name, units)
		if allowed {
			return
		}
		// The in-code mark is the more specific claim: keep its counts when
		// both declined.
		if declinedUnits == nil {
			declinedUnits = declined
		}
	}
	statements := make([]StatementRecord, len(b.statements))
	copy(statements, b.statements)
	v := Violation{
		Boundary:            b.name,
		WriteUnits:          units,
		Statements:          statements,
		TruncatedStatements: b.truncated,
		AllowedWriteUnits:   declinedUnits,
		Attrs:               b.attrs,
	}
	for _, r := range d.reporters {
		r.Report(b, v)
	}
}
