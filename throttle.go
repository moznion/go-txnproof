package txnproof

import (
	"context"
	"strings"
	"sync"
	"time"
)

const (
	// maxUnboundedWriteKeys bounds how many distinct statement keys the
	// unbounded-write throttle tracks. Boundary names are a small,
	// code-defined set, but statement texts are open-ended (e.g. queries
	// built without placeholders), so the statement-keyed map is capped:
	// once full, reports for statements not already tracked are forwarded
	// unthrottled rather than growing the map without bound.
	maxUnboundedWriteKeys = 1024

	// unboundedWriteKeyLen is the maximum length of a statement throttle
	// key. Keys are whitespace-normalized query prefixes; they are used
	// only for deduplication, never displayed.
	unboundedWriteKeyLen = 200
)

// ThrottlingReporter wraps another Reporter and deduplicates repeated reports,
// so that a violating boundary on a hot path does not fire the wrapped
// reporter on every request. Intended for production monitoring.
//
// Per boundary name, the first Violation is forwarded to the wrapped reporter
// immediately; subsequent Violations for the same boundary within the
// configured interval are suppressed; once the interval has elapsed, the next
// Violation is forwarded again and a new interval starts.
//
// The optional reporter extensions are throttled with the same interval but
// with their own keys and independent windows:
//
//   - Unbounded writes (UnboundedWriteReporter) are throttled per statement,
//     keyed by the whitespace-normalized query text (truncated, see
//     unboundedWriteKeyLen) — a hot-path unbounded write repeats the same
//     statement text, so the statement is the natural dedup unit.
//   - Stale AllowNonAtomic marks (StaleAllowReporter) are throttled per
//     boundary name, independently of that boundary's Violation window —
//     stale-allow reports are per execution and therefore just as noisy on a
//     hot path as violations.
//   - Nested boundaries (NestedBoundaryReporter) are throttled per
//     outer/inner name pair — overlapping instrumentation layers repeat the
//     same pair on every request.
//
// Each extension is forwarded only when the wrapped reporter implements the
// corresponding interface, so wrapping neither swallows nor fabricates those
// signals.
//
// Suppressed reports are not silently lost: cumulative per-key suppression
// counts are available via SuppressedViolations, SuppressedUnboundedWrites,
// and SuppressedStaleAllows, meant to be polled periodically (e.g. logged or
// exported as metrics on a ticker) to recover the true report volume.
//
// Memory stays bounded: the two boundary-keyed maps grow with the set of
// boundary names, which is code-defined and small in practice; the
// statement-keyed map is capped at maxUnboundedWriteKeys.
type ThrottlingReporter struct {
	next     Reporter
	interval time.Duration
	now      func() time.Time // injectable clock for tests

	mu          sync.Mutex
	violations  map[string]*throttleState
	staleAllows map[string]*throttleState
	unbounded   map[string]*throttleState
	nested      map[string]*throttleState
}

var (
	_ Reporter               = (*ThrottlingReporter)(nil)
	_ UnboundedWriteReporter = (*ThrottlingReporter)(nil)
	_ StaleAllowReporter     = (*ThrottlingReporter)(nil)
	_ NestedBoundaryReporter = (*ThrottlingReporter)(nil)
)

// throttleState tracks one throttle key: when a report was last forwarded and
// how many reports have been suppressed in total since creation.
type throttleState struct {
	lastForwarded time.Time
	suppressed    int
}

// NewThrottlingReporter wraps next so that repeated reports for the same key
// (boundary name for violations and stale allows, statement text for
// unbounded writes) are forwarded at most once per interval. A non-positive
// interval disables throttling: every report is forwarded.
func NewThrottlingReporter(next Reporter, interval time.Duration) *ThrottlingReporter {
	return &ThrottlingReporter{
		next:        next,
		interval:    interval,
		now:         time.Now,
		violations:  map[string]*throttleState{},
		staleAllows: map[string]*throttleState{},
		unbounded:   map[string]*throttleState{},
		nested:      map[string]*throttleState{},
	}
}

// admit decides whether a report for key should be forwarded now, updating
// the throttle window and the suppression count. maxKeys > 0 caps the number
// of distinct tracked keys; reports for untracked keys beyond the cap are
// forwarded unthrottled (failing open loses dedup, never reports).
func (r *ThrottlingReporter) admit(m map[string]*throttleState, key string, maxKeys int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	st, ok := m[key]
	if !ok {
		if maxKeys > 0 && len(m) >= maxKeys {
			return true
		}
		m[key] = &throttleState{lastForwarded: r.now()}
		return true
	}
	now := r.now()
	if now.Sub(st.lastForwarded) >= r.interval {
		st.lastForwarded = now
		return true
	}
	st.suppressed++
	return false
}

// Report forwards the first Violation per boundary immediately and at most
// one more per interval afterwards; the rest are counted as suppressed.
func (r *ThrottlingReporter) Report(ctx context.Context, v Violation) {
	if r.interval <= 0 || r.admit(r.violations, v.Boundary, 0) {
		r.next.Report(ctx, v)
	}
}

// ReportUnboundedWrite forwards unbounded write reports throttled per
// statement text. It is a no-op when the wrapped reporter does not implement
// UnboundedWriteReporter.
func (r *ThrottlingReporter) ReportUnboundedWrite(ctx context.Context, s StatementRecord) {
	ur, ok := r.next.(UnboundedWriteReporter)
	if !ok {
		return
	}
	if r.interval <= 0 || r.admit(r.unbounded, unboundedWriteKey(s.Query), maxUnboundedWriteKeys) {
		ur.ReportUnboundedWrite(ctx, s)
	}
}

// ReportStaleAllow forwards stale AllowNonAtomic reports throttled per
// boundary name (independently of the boundary's Violation window). It is a
// no-op when the wrapped reporter does not implement StaleAllowReporter.
func (r *ThrottlingReporter) ReportStaleAllow(ctx context.Context, s StaleAllow) {
	sr, ok := r.next.(StaleAllowReporter)
	if !ok {
		return
	}
	if r.interval <= 0 || r.admit(r.staleAllows, s.Boundary, 0) {
		sr.ReportStaleAllow(ctx, s)
	}
}

// ReportNestedBoundary forwards nested-boundary occurrences throttled per
// outer/inner name pair. It is a no-op when the wrapped reporter does not
// implement NestedBoundaryReporter.
func (r *ThrottlingReporter) ReportNestedBoundary(ctx context.Context, n NestedBoundary) {
	nr, ok := r.next.(NestedBoundaryReporter)
	if !ok {
		return
	}
	if r.interval <= 0 || r.admit(r.nested, n.Outer+"\x00"+n.Inner, 0) {
		nr.ReportNestedBoundary(ctx, n)
	}
}

// SuppressedViolations returns the cumulative number of suppressed Violations
// per boundary name since the reporter was created. Counts only grow;
// boundaries with zero suppressions are omitted. Poll it periodically to
// recover the true violation volume behind the throttled stream.
func (r *ThrottlingReporter) SuppressedViolations() map[string]int {
	return r.snapshot(r.violations)
}

// SuppressedUnboundedWrites returns the cumulative number of suppressed
// unbounded write reports per statement key (whitespace-normalized, possibly
// truncated query text) since the reporter was created.
func (r *ThrottlingReporter) SuppressedUnboundedWrites() map[string]int {
	return r.snapshot(r.unbounded)
}

// SuppressedStaleAllows returns the cumulative number of suppressed stale
// AllowNonAtomic reports per boundary name since the reporter was created.
func (r *ThrottlingReporter) SuppressedStaleAllows() map[string]int {
	return r.snapshot(r.staleAllows)
}

// SuppressedNestedBoundaries returns the cumulative number of suppressed
// nested-boundary reports per "outer\x00inner" name pair.
func (r *ThrottlingReporter) SuppressedNestedBoundaries() map[string]int {
	return r.snapshot(r.nested)
}

func (r *ThrottlingReporter) snapshot(m map[string]*throttleState) map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]int, len(m))
	for k, st := range m {
		if st.suppressed > 0 {
			out[k] = st.suppressed
		}
	}
	return out
}

// unboundedWriteKey derives the throttle key for an unbounded write:
// whitespace-normalized query text, truncated to unboundedWriteKeyLen bytes.
func unboundedWriteKey(query string) string {
	k := strings.Join(strings.Fields(query), " ")
	if len(k) > unboundedWriteKeyLen {
		k = k[:unboundedWriteKeyLen]
	}
	return k
}
