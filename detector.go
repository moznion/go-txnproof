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

	name        string
	allowed     bool // marked intentionally non-atomic via AllowNonAtomic
	allowReason string
	attrs       []BoundaryAttr // immutable after StartBoundary returns

	mu         sync.Mutex
	statements []StatementRecord
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
// Rot prevention works per execution instead of per entry: when an allowed
// boundary finishes with fewer than 2 write units (i.e. the allow suppressed
// nothing), reporters that implement StaleAllowReporter are notified — the
// same discipline as unused //nolint directives.
func AllowNonAtomic(reason string) BoundaryOption {
	return func(b *Boundary) {
		b.allowed = true
		b.allowReason = reason
	}
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
	b.mu.Unlock()

	// After finished is set under the lock, record returns early without ever
	// touching b.statements / b.truncated again, so both are immutable from here
	// on and can be read without the lock. Deferring the statement snapshot to
	// the violation path below means the healthy, non-violating majority of
	// boundaries never pay for the make+copy at all.

	if units < 2 {
		if b.allowed {
			sa := StaleAllow{Boundary: b.name, Reason: b.allowReason, WriteUnits: units}
			for _, r := range d.reporters {
				if sr, ok := r.(StaleAllowReporter); ok {
					sr.ReportStaleAllow(b, sa)
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
	statements := make([]StatementRecord, len(b.statements))
	copy(statements, b.statements)
	v := Violation{
		Boundary:            b.name,
		WriteUnits:          units,
		Statements:          statements,
		TruncatedStatements: b.truncated,
		Attrs:               b.attrs,
	}
	for _, r := range d.reporters {
		r.Report(b, v)
	}
}
