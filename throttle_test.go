package txnproof

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeClock is a mutex-guarded manual clock for deterministic throttle tests.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func setupThrottle(interval time.Duration) (*ThrottlingReporter, *CollectingReporter, *fakeClock) {
	cr := NewCollectingReporter()
	tr := NewThrottlingReporter(cr, interval)
	clock := newFakeClock()
	tr.now = clock.Now
	return tr, cr, clock
}

func TestThrottlingReporterSuppressesWithinInterval(t *testing.T) {
	tr, cr, clock := setupThrottle(time.Minute)
	ctx := context.Background()
	v := Violation{Boundary: "CreateUser", WriteUnits: 2}

	tr.Report(ctx, v)
	clock.Advance(10 * time.Second)
	tr.Report(ctx, v)
	clock.Advance(10 * time.Second)
	tr.Report(ctx, v)

	if got := len(cr.Violations()); got != 1 {
		t.Fatalf("expected 1 forwarded violation, got %d", got)
	}
	if got := tr.SuppressedViolations(); got["CreateUser"] != 2 {
		t.Fatalf("expected 2 suppressed for CreateUser, got %v", got)
	}
}

func TestThrottlingReporterForwardsAgainAfterInterval(t *testing.T) {
	tr, cr, clock := setupThrottle(time.Minute)
	ctx := context.Background()
	v := Violation{Boundary: "CreateUser", WriteUnits: 2}

	tr.Report(ctx, v)
	clock.Advance(30 * time.Second)
	tr.Report(ctx, v) // suppressed
	clock.Advance(30 * time.Second)
	tr.Report(ctx, v) // one full interval since the first forward -> forwarded

	if got := len(cr.Violations()); got != 2 {
		t.Fatalf("expected 2 forwarded violations, got %d", got)
	}

	// The second forward opened a fresh window.
	tr.Report(ctx, v)
	if got := len(cr.Violations()); got != 2 {
		t.Fatalf("expected the fresh window to suppress, got %d forwarded", got)
	}
	if got := tr.SuppressedViolations(); got["CreateUser"] != 2 {
		t.Fatalf("expected 2 suppressed in total, got %v", got)
	}
}

func TestThrottlingReporterThrottlesBoundariesIndependently(t *testing.T) {
	tr, cr, _ := setupThrottle(time.Minute)
	ctx := context.Background()

	tr.Report(ctx, Violation{Boundary: "A", WriteUnits: 2})
	tr.Report(ctx, Violation{Boundary: "A", WriteUnits: 2})
	tr.Report(ctx, Violation{Boundary: "B", WriteUnits: 2})

	vs := cr.Violations()
	if len(vs) != 2 || vs[0].Boundary != "A" || vs[1].Boundary != "B" {
		t.Fatalf("expected first A and first B forwarded, got %+v", vs)
	}
	got := tr.SuppressedViolations()
	if got["A"] != 1 || got["B"] != 0 {
		t.Fatalf("unexpected suppressed counts: %v", got)
	}
}

func TestThrottlingReporterNonPositiveIntervalForwardsEverything(t *testing.T) {
	tr, cr, _ := setupThrottle(0)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		tr.Report(ctx, Violation{Boundary: "A", WriteUnits: 2})
	}
	if got := len(cr.Violations()); got != 3 {
		t.Fatalf("expected 3 forwarded violations, got %d", got)
	}
	if got := tr.SuppressedViolations(); len(got) != 0 {
		t.Fatalf("expected no suppressed counts, got %v", got)
	}
}

func TestThrottlingReporterStaleAllowsThrottledPerBoundaryIndependentlyOfViolations(t *testing.T) {
	tr, cr, _ := setupThrottle(time.Minute)
	ctx := context.Background()

	// A violation for the boundary must not consume the stale-allow window.
	tr.Report(ctx, Violation{Boundary: "A", WriteUnits: 2})
	tr.ReportStaleAllow(ctx, StaleAllow{Boundary: "A", Reason: "r", WriteUnits: 1})
	tr.ReportStaleAllow(ctx, StaleAllow{Boundary: "A", Reason: "r", WriteUnits: 1})
	tr.ReportStaleAllow(ctx, StaleAllow{Boundary: "B", Reason: "r", WriteUnits: 0})

	if got := len(cr.Violations()); got != 1 {
		t.Fatalf("expected 1 forwarded violation, got %d", got)
	}
	sas := cr.StaleAllows()
	if len(sas) != 2 || sas[0].Boundary != "A" || sas[1].Boundary != "B" {
		t.Fatalf("expected first stale allow per boundary forwarded, got %+v", sas)
	}
	if got := tr.SuppressedStaleAllows(); got["A"] != 1 {
		t.Fatalf("expected 1 suppressed stale allow for A, got %v", got)
	}
}

func TestThrottlingReporterUnboundedWritesThrottledPerNormalizedStatement(t *testing.T) {
	tr, cr, _ := setupThrottle(time.Minute)
	ctx := context.Background()

	tr.ReportUnboundedWrite(ctx, StatementRecord{Query: "INSERT INTO t VALUES (1)", Kind: KindWrite})
	// Same statement modulo whitespace -> same key -> suppressed.
	tr.ReportUnboundedWrite(ctx, StatementRecord{Query: "INSERT   INTO t\n VALUES (1)", Kind: KindWrite})
	// Different statement -> its own window -> forwarded.
	tr.ReportUnboundedWrite(ctx, StatementRecord{Query: "DELETE FROM t", Kind: KindWrite})

	uw := cr.UnboundedWrites()
	if len(uw) != 2 {
		t.Fatalf("expected 2 forwarded unbounded writes, got %+v", uw)
	}
	got := tr.SuppressedUnboundedWrites()
	if got["INSERT INTO t VALUES (1)"] != 1 {
		t.Fatalf("unexpected suppressed counts: %v", got)
	}
}

func TestThrottlingReporterUnboundedWriteKeyCapFailsOpen(t *testing.T) {
	tr, cr, _ := setupThrottle(time.Minute)
	ctx := context.Background()

	for i := 0; i < maxUnboundedWriteKeys; i++ {
		tr.ReportUnboundedWrite(ctx, StatementRecord{Query: fmt.Sprintf("INSERT INTO t VALUES (%d)", i), Kind: KindWrite})
	}
	if got := len(cr.UnboundedWrites()); got != maxUnboundedWriteKeys {
		t.Fatalf("expected %d forwarded unbounded writes, got %d", maxUnboundedWriteKeys, got)
	}

	// Beyond the cap, untracked statements are forwarded every time instead
	// of growing the map.
	overflow := StatementRecord{Query: "UPDATE overflow SET x = 1", Kind: KindWrite}
	tr.ReportUnboundedWrite(ctx, overflow)
	tr.ReportUnboundedWrite(ctx, overflow)
	if got := len(cr.UnboundedWrites()); got != maxUnboundedWriteKeys+2 {
		t.Fatalf("expected overflow statements to be forwarded unthrottled, got %d forwarded", got)
	}

	// Already-tracked statements are still throttled.
	tr.ReportUnboundedWrite(ctx, StatementRecord{Query: "INSERT INTO t VALUES (0)", Kind: KindWrite})
	if got := len(cr.UnboundedWrites()); got != maxUnboundedWriteKeys+2 {
		t.Fatalf("expected tracked statement to stay throttled, got %d forwarded", got)
	}
}

// plainReporter implements only Reporter, none of the optional extensions.
type plainReporter struct {
	mu sync.Mutex
	n  int
}

func (r *plainReporter) Report(context.Context, Violation) {
	r.mu.Lock()
	r.n++
	r.mu.Unlock()
}

func TestThrottlingReporterWithPlainNextIgnoresOptionalSignals(t *testing.T) {
	next := &plainReporter{}
	tr := NewThrottlingReporter(next, time.Minute)
	ctx := context.Background()

	// Must not panic and must not track anything.
	tr.ReportUnboundedWrite(ctx, StatementRecord{Query: "INSERT INTO t VALUES (1)", Kind: KindWrite})
	tr.ReportStaleAllow(ctx, StaleAllow{Boundary: "A"})
	tr.Report(ctx, Violation{Boundary: "A", WriteUnits: 2})

	if next.n != 1 {
		t.Fatalf("expected 1 forwarded violation, got %d", next.n)
	}
	if got := tr.SuppressedUnboundedWrites(); len(got) != 0 {
		t.Fatalf("expected no unbounded-write tracking, got %v", got)
	}
	if got := tr.SuppressedStaleAllows(); len(got) != 0 {
		t.Fatalf("expected no stale-allow tracking, got %v", got)
	}
}

func TestThrottlingReporterConcurrentReports(t *testing.T) {
	tr, cr, _ := setupThrottle(time.Minute)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				tr.Report(ctx, Violation{Boundary: "hot", WriteUnits: 2})
				tr.ReportStaleAllow(ctx, StaleAllow{Boundary: "allowed"})
				tr.ReportUnboundedWrite(ctx, StatementRecord{Query: "INSERT INTO t VALUES (1)", Kind: KindWrite})
				_ = tr.SuppressedViolations()
			}
		}(i)
	}
	wg.Wait()

	if got := len(cr.Violations()); got != 1 {
		t.Fatalf("expected exactly 1 forwarded violation, got %d", got)
	}
	if got := tr.SuppressedViolations(); got["hot"] != 799 {
		t.Fatalf("expected 799 suppressed violations, got %v", got)
	}
}

func TestThrottlingReporterEndToEndWithDetector(t *testing.T) {
	cr := NewCollectingReporter()
	tr := NewThrottlingReporter(cr, time.Minute)
	tr.now = newFakeClock().Now
	det := New(WithReporter(tr))
	db := det.NewNullDB()
	t.Cleanup(func() { _ = db.Close() })

	violate := func() {
		ctx, finish := det.StartBoundary(context.Background(), "CreateUser")
		defer finish.Finish()
		mustExec(t, db, ctx, "INSERT INTO users (id) VALUES (1)")
		mustExec(t, db, ctx, "UPDATE counters SET n = n + 1")
	}
	violate()
	violate()

	if got := len(cr.Violations()); got != 1 {
		t.Fatalf("expected the repeated boundary to be reported once, got %d", got)
	}
	if got := tr.SuppressedViolations(); got["CreateUser"] != 1 {
		t.Fatalf("expected 1 suppressed violation, got %v", got)
	}
}
