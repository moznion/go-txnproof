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

// boundary accumulates the statement timeline of one logical execution.
type boundary struct {
	name        string
	allowed     bool // marked intentionally non-atomic via AllowNonAtomic
	allowReason string
	attrs       []BoundaryAttr // immutable after StartBoundary returns

	mu               sync.Mutex
	statements       []StatementRecord
	writeTxUnits     map[uint64]struct{} // transactions that contained >=1 write
	autoCommitWrites int                 // each auto-commit write is its own atomic unit
	truncated        int
	finished         bool
}

// BoundaryOption configures a single boundary at StartBoundary / InBoundary.
type BoundaryOption func(*boundary)

// AllowNonAtomic marks the boundary as intentionally non-atomic, suppressing
// its Violation at the call site — the in-code alternative to a central
// Allowlist entry. The reason should say why and reference a ticket.
//
// Rot prevention works per execution instead of per entry: when an allowed
// boundary finishes with fewer than 2 write units (i.e. the allow suppressed
// nothing), reporters that implement StaleAllowReporter are notified — the
// same discipline as unused //nolint directives.
func AllowNonAtomic(reason string) BoundaryOption {
	return func(b *boundary) {
		b.allowed = true
		b.allowReason = reason
	}
}

// StartBoundary marks the beginning of a logical boundary (a use case, a
// request handler, a job) on the context. Every statement executed through a
// wrapped driver with the returned context is attributed to this boundary.
//
// The returned finish function evaluates the boundary and reports a Violation
// if its writes span two or more atomic units. Call it exactly when the
// boundary ends (typically via defer); it is idempotent.
//
// Starting a boundary on a context that already carries one shadows the outer
// boundary for statements executed with the new context (reported when
// WithNestedBoundaryDetection is on).
func (d *Detector) StartBoundary(ctx context.Context, name string, opts ...BoundaryOption) (context.Context, func()) {
	if outer, _ := ctx.Value(boundaryCtxKey{}).(*boundary); outer != nil && d.reportNested {
		n := NestedBoundary{Outer: outer.name, Inner: name, Time: time.Now()}
		for _, r := range d.reporters {
			if nr, ok := r.(NestedBoundaryReporter); ok {
				nr.ReportNestedBoundary(ctx, n)
			}
		}
	}
	b := &boundary{
		name:         name,
		writeTxUnits: map[uint64]struct{}{},
	}
	if d.attrsFunc != nil {
		// Evaluated once per boundary, before per-boundary options so that
		// WithBoundaryAttrs entries come after detector-level ones.
		b.attrs = append(b.attrs, d.attrsFunc(ctx)...)
	}
	for _, o := range opts {
		o(b)
	}
	bctx := context.WithValue(ctx, boundaryCtxKey{}, b)
	return bctx, func() { d.finishBoundary(bctx, b) }
}

// InBoundary runs f inside a boundary and finishes it when f returns.
func (d *Detector) InBoundary(ctx context.Context, name string, f func(context.Context) error, opts ...BoundaryOption) error {
	ctx, finish := d.StartBoundary(ctx, name, opts...)
	defer finish()
	return f(ctx)
}

// record attributes one executed statement to the boundary in ctx (if any).
func (d *Detector) record(ctx context.Context, query string, kind StatementKind, txID uint64) {
	b, _ := ctx.Value(boundaryCtxKey{}).(*boundary)
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
		b.statements = append(b.statements, StatementRecord{Query: query, Kind: kind, TxID: txID, Time: time.Now()})
	} else {
		b.truncated++
	}
	if kind == KindWrite {
		if txID != 0 {
			b.writeTxUnits[txID] = struct{}{}
		} else {
			b.autoCommitWrites++
		}
	}
}

func (d *Detector) finishBoundary(ctx context.Context, b *boundary) {
	b.mu.Lock()
	if b.finished {
		b.mu.Unlock()
		return
	}
	b.finished = true
	units := len(b.writeTxUnits) + b.autoCommitWrites
	statements := make([]StatementRecord, len(b.statements))
	copy(statements, b.statements)
	truncated := b.truncated
	b.mu.Unlock()

	if units < 2 {
		if b.allowed {
			sa := StaleAllow{Boundary: b.name, Reason: b.allowReason, WriteUnits: units}
			for _, r := range d.reporters {
				if sr, ok := r.(StaleAllowReporter); ok {
					sr.ReportStaleAllow(ctx, sa)
				}
			}
		}
		return
	}
	if b.allowed {
		return
	}
	if d.allowlist != nil && d.allowlist.allow(b.name) {
		return
	}
	v := Violation{
		Boundary:            b.name,
		WriteUnits:          units,
		Statements:          statements,
		TruncatedStatements: truncated,
		Attrs:               b.attrs,
	}
	for _, r := range d.reporters {
		r.Report(ctx, v)
	}
}
