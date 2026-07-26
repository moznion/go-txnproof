package txnproof

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func setup(t *testing.T, opts ...Option) (*Detector, *CollectingReporter, *sql.DB) {
	t.Helper()
	cr := NewCollectingReporter()
	det := New(append([]Option{WithReporter(cr)}, opts...)...)
	db := det.NewNullDB()
	t.Cleanup(func() { _ = db.Close() })
	return det, cr, db
}

func TestTwoAutoCommitWritesIsViolation(t *testing.T) {
	det, cr, db := setup(t)

	ctx, finish := det.StartBoundary(context.Background(), "CreateUser")
	mustExec(t, db, ctx, "INSERT INTO users (id) VALUES (1)")
	mustExec(t, db, ctx, "UPDATE counters SET n = n + 1")
	finish.Finish()

	vs := cr.Violations()
	if len(vs) != 1 {
		t.Fatalf("expected 1 violation, got %d", len(vs))
	}
	if vs[0].Boundary != "CreateUser" || vs[0].WriteUnits != 2 {
		t.Fatalf("unexpected violation: %+v", vs[0])
	}
	if !strings.Contains(vs[0].String(), "auto-commit") {
		t.Errorf("String() should mention auto-commit: %s", vs[0].String())
	}
}

func TestWriteTxUnitCountingInlineAndOverflow(t *testing.T) {
	// Exercises noteWriteTx across the inline capacity boundary: each distinct
	// transaction ID counts once, repeats are deduplicated, and IDs beyond the
	// inline array spill into the overflow map without being lost or doubled.
	det, cr, _ := setup(t)

	ctx, b := det.StartBoundary(context.Background(), "ManyTx")
	// 6 distinct write transactions (> inlineWriteTxCap == 4), each recorded
	// twice to prove dedup, plus one auto-commit write.
	for tx := uint64(1); tx <= 6; tx++ {
		det.record(ctx, "INSERT INTO t VALUES (1)", KindWrite, tx)
		det.record(ctx, "UPDATE t SET a = 1", KindWrite, tx)
	}
	det.record(ctx, "INSERT INTO t VALUES (2)", KindWrite, 0)
	// Reads never count, in a tx or not.
	det.record(ctx, "SELECT 1", KindRead, 3)
	b.Finish()

	vs := cr.Violations()
	if len(vs) != 1 {
		t.Fatalf("expected 1 violation, got %d", len(vs))
	}
	// 6 write transactions + 1 auto-commit write = 7 atomic units.
	if got := vs[0].WriteUnits; got != 7 {
		t.Fatalf("WriteUnits = %d, want 7", got)
	}
}

func TestSingleWriteIsNotViolation(t *testing.T) {
	det, cr, db := setup(t)

	ctx, finish := det.StartBoundary(context.Background(), "CreateUser")
	mustExec(t, db, ctx, "INSERT INTO users (id) VALUES (1)")
	finish.Finish()

	if vs := cr.Violations(); len(vs) != 0 {
		t.Fatalf("expected no violations, got %+v", vs)
	}
}

func TestWritesInsideSingleTxIsNotViolation(t *testing.T) {
	det, cr, db := setup(t)

	ctx, finish := det.StartBoundary(context.Background(), "CreateUser")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO users (id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE counters SET n = n + 1"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	finish.Finish()

	if vs := cr.Violations(); len(vs) != 0 {
		t.Fatalf("expected no violations, got %+v", vs)
	}
}

func TestTxPlusAutoCommitWriteIsViolation(t *testing.T) {
	det, cr, db := setup(t)

	ctx, finish := det.StartBoundary(context.Background(), "CreateUser")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO users (id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, ctx, "UPDATE counters SET n = n + 1")
	finish.Finish()

	vs := cr.Violations()
	if len(vs) != 1 || vs[0].WriteUnits != 2 {
		t.Fatalf("expected 1 violation with 2 units, got %+v", vs)
	}
}

func TestTwoSeparateTxsIsViolation(t *testing.T) {
	det, cr, db := setup(t)

	ctx, finish := det.StartBoundary(context.Background(), "CreateUser")
	for i := 0; i < 2; i++ {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO users (id) VALUES (1)"); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	finish.Finish()

	vs := cr.Violations()
	if len(vs) != 1 || vs[0].WriteUnits != 2 {
		t.Fatalf("expected 1 violation with 2 units, got %+v", vs)
	}
}

func TestRolledBackTxStillCountsAsUnit(t *testing.T) {
	// A rollback means the boundary already attempted a partial write path;
	// the boundary is still structurally non-atomic.
	det, cr, db := setup(t)

	ctx, finish := det.StartBoundary(context.Background(), "CreateUser")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO users (id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, ctx, "INSERT INTO audit (id) VALUES (1)")
	finish.Finish()

	if vs := cr.Violations(); len(vs) != 1 {
		t.Fatalf("expected 1 violation, got %+v", vs)
	}
}

func TestReadsDoNotCount(t *testing.T) {
	det, cr, db := setup(t)

	ctx, finish := det.StartBoundary(context.Background(), "ShowUser")
	mustQuery(t, db, ctx, "SELECT * FROM users")
	mustQuery(t, db, ctx, "SELECT * FROM orders")
	mustExec(t, db, ctx, "UPDATE users SET last_seen = now()")
	finish.Finish()

	if vs := cr.Violations(); len(vs) != 0 {
		t.Fatalf("expected no violations, got %+v", vs)
	}
}

func TestPreparedStatements(t *testing.T) {
	det, cr, db := setup(t)

	ctx, finish := det.StartBoundary(context.Background(), "CreateUser")
	stmt, err := db.PrepareContext(ctx, "INSERT INTO users (id) VALUES ($1)")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stmt.Close() }()
	if _, err := stmt.ExecContext(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.ExecContext(ctx, 2); err != nil {
		t.Fatal(err)
	}
	finish.Finish()

	vs := cr.Violations()
	if len(vs) != 1 || vs[0].WriteUnits != 2 {
		t.Fatalf("expected 1 violation with 2 units, got %+v", vs)
	}
}

// TestPreparedStatementClassifiedOnce pins the prepared-statement fast path:
// the query is classified once at Prepare, never again per execution — a
// reused prepared data-modifying CTE would otherwise re-pay a full-text token
// scan on every execution. The violation assertion proves the executions
// still went through the wrapped observation path.
func TestPreparedStatementClassifiedOnce(t *testing.T) {
	var calls atomic.Int32
	det, cr, db := setup(t, WithClassifier(func(q string) StatementKind {
		calls.Add(1)
		return DefaultClassifier(q)
	}))
	// A single connection keeps database/sql from re-preparing the statement
	// on another pooled connection, which would legitimately classify again.
	db.SetMaxOpenConns(1)

	ctx, finish := det.StartBoundary(context.Background(), "CreateUser")
	stmt, err := db.PrepareContext(ctx, "INSERT INTO users (id) VALUES ($1)")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stmt.Close() }()
	for i := 0; i < 3; i++ {
		if _, err := stmt.ExecContext(ctx, i); err != nil {
			t.Fatal(err)
		}
	}
	finish.Finish()

	vs := cr.Violations()
	if len(vs) != 1 || vs[0].WriteUnits != 3 {
		t.Fatalf("expected 1 violation with 3 units, got %+v", vs)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("classifier called %d times for 1 prepare + 3 executions, want 1", got)
	}
}

func TestAllowlistSuppressesAndTracksUsage(t *testing.T) {
	al := NewAllowlist().
		Add("BestEffortAudit", "audit writes are intentionally best-effort (TICKET-123)").
		Add("NeverViolates", "stale entry")
	det, cr, db := setup(t, WithAllowlist(al))

	ctx, finish := det.StartBoundary(context.Background(), "BestEffortAudit")
	mustExec(t, db, ctx, "INSERT INTO a (id) VALUES (1)")
	mustExec(t, db, ctx, "INSERT INTO b (id) VALUES (1)")
	finish.Finish()

	if vs := cr.Violations(); len(vs) != 0 {
		t.Fatalf("allowlisted boundary should be suppressed, got %+v", vs)
	}
	unused := al.UnusedEntries()
	if len(unused) != 1 || unused[0] != "NeverViolates" {
		t.Fatalf("expected [NeverViolates] as unused, got %v", unused)
	}
}

func TestAllowNonAtomicSuppressesViolation(t *testing.T) {
	det, cr, db := setup(t)

	ctx, finish := det.StartBoundary(context.Background(), "BestEffortAudit",
		AllowNonAtomic("audit writes are intentionally best-effort (TICKET-123)"))
	mustExec(t, db, ctx, "INSERT INTO a (id) VALUES (1)")
	mustExec(t, db, ctx, "INSERT INTO audit (id) VALUES (1)")
	finish.Finish()

	if vs := cr.Violations(); len(vs) != 0 {
		t.Fatalf("allowed boundary should be suppressed, got %+v", vs)
	}
	if sa := cr.StaleAllows(); len(sa) != 0 {
		t.Fatalf("allow actually suppressed a violation; must not be stale, got %+v", sa)
	}
}

func TestAllowNonAtomicStaleIsReported(t *testing.T) {
	det, cr, db := setup(t)

	err := det.InBoundary(context.Background(), "BestEffortAudit", func(ctx context.Context) error {
		mustExec(t, db, ctx, "INSERT INTO a (id) VALUES (1)")
		return nil
	}, AllowNonAtomic("audit writes are intentionally best-effort (TICKET-123)"))
	if err != nil {
		t.Fatal(err)
	}

	sa := cr.StaleAllows()
	if len(sa) != 1 {
		t.Fatalf("expected 1 stale allow, got %+v", sa)
	}
	if sa[0].Boundary != "BestEffortAudit" || sa[0].WriteUnits != 1 || !strings.Contains(sa[0].Reason, "TICKET-123") {
		t.Fatalf("unexpected stale allow: %+v", sa[0])
	}
	if vs := cr.Violations(); len(vs) != 0 {
		t.Fatalf("expected no violations, got %+v", vs)
	}

	ft := &fakeT{}
	cr.RequireNoStaleAllows(ft)
	if len(ft.errors) != 1 {
		t.Fatalf("expected 1 test error from RequireNoStaleAllows, got %d", len(ft.errors))
	}
}

func TestUnboundedWriteDetection(t *testing.T) {
	_, cr, db := setup(t, WithUnboundedWriteDetection())

	// no boundary on this context
	if _, err := db.ExecContext(context.Background(), "INSERT INTO users (id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}

	uw := cr.UnboundedWrites()
	if len(uw) != 1 {
		t.Fatalf("expected 1 unbounded write, got %d", len(uw))
	}
	if vs := cr.Violations(); len(vs) != 0 {
		t.Fatalf("unbounded writes must not be violations, got %+v", vs)
	}
}

func TestFinishIsIdempotent(t *testing.T) {
	det, cr, db := setup(t)

	ctx, finish := det.StartBoundary(context.Background(), "CreateUser")
	mustExec(t, db, ctx, "INSERT INTO a (id) VALUES (1)")
	mustExec(t, db, ctx, "INSERT INTO b (id) VALUES (1)")
	finish.Finish()
	finish.Finish()

	if vs := cr.Violations(); len(vs) != 1 {
		t.Fatalf("expected exactly 1 violation, got %d", len(vs))
	}
}

func TestInBoundary(t *testing.T) {
	det, cr, db := setup(t)

	err := det.InBoundary(context.Background(), "CreateUser", func(ctx context.Context) error {
		mustExec(t, db, ctx, "INSERT INTO a (id) VALUES (1)")
		mustExec(t, db, ctx, "INSERT INTO b (id) VALUES (1)")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if vs := cr.Violations(); len(vs) != 1 {
		t.Fatalf("expected 1 violation, got %d", len(vs))
	}
}

func TestStatementRecordCap(t *testing.T) {
	det, cr, db := setup(t, WithMaxRecordedStatements(3))

	ctx, finish := det.StartBoundary(context.Background(), "Bulk")
	for i := 0; i < 5; i++ {
		mustExec(t, db, ctx, "INSERT INTO a (id) VALUES (1)")
	}
	finish.Finish()

	vs := cr.Violations()
	if len(vs) != 1 {
		t.Fatalf("expected 1 violation, got %d", len(vs))
	}
	if vs[0].WriteUnits != 5 {
		t.Errorf("unit counting must not be truncated: got %d", vs[0].WriteUnits)
	}
	if len(vs[0].Statements) != 3 || vs[0].TruncatedStatements != 2 {
		t.Errorf("expected 3 recorded / 2 truncated, got %d / %d", len(vs[0].Statements), vs[0].TruncatedStatements)
	}
}

func TestConcurrentWritesInOneBoundary(t *testing.T) {
	det, cr, db := setup(t)

	ctx, finish := det.StartBoundary(context.Background(), "Concurrent")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = db.ExecContext(ctx, "INSERT INTO a (id) VALUES (1)")
		}()
	}
	wg.Wait()
	finish.Finish()

	vs := cr.Violations()
	if len(vs) != 1 || vs[0].WriteUnits != 8 {
		t.Fatalf("expected 1 violation with 8 units, got %+v", vs)
	}
}

func TestRequireNoViolations(t *testing.T) {
	det, cr, db := setup(t)

	ctx, finish := det.StartBoundary(context.Background(), "CreateUser")
	mustExec(t, db, ctx, "INSERT INTO a (id) VALUES (1)")
	mustExec(t, db, ctx, "INSERT INTO b (id) VALUES (1)")
	finish.Finish()

	ft := &fakeT{}
	cr.RequireNoViolations(ft)
	if len(ft.errors) != 1 {
		t.Fatalf("expected 1 test error, got %d", len(ft.errors))
	}
}

func TestRequireNoUnboundedWrites(t *testing.T) {
	det, cr, db := setup(t, WithUnboundedWriteDetection())

	// A read with no boundary must not trip it.
	mustQuery(t, db, context.Background(), "SELECT id FROM a")
	ft := &fakeT{}
	cr.RequireNoUnboundedWrites(ft)
	if len(ft.errors) != 0 {
		t.Fatalf("expected no test errors for a boundary-less read, got %d", len(ft.errors))
	}

	// A write inside a boundary must not trip it either.
	ctx, finish := det.StartBoundary(context.Background(), "CreateUser")
	mustExec(t, db, ctx, "INSERT INTO a (id) VALUES (1)")
	finish.Finish()
	cr.RequireNoUnboundedWrites(ft)
	if len(ft.errors) != 0 {
		t.Fatalf("expected no test errors for a bounded write, got %d", len(ft.errors))
	}

	// A single write with no boundary trips it: one error per write.
	mustExec(t, db, context.Background(), "INSERT INTO a (id) VALUES (2)")
	cr.RequireNoUnboundedWrites(ft)
	if len(ft.errors) != 1 {
		t.Fatalf("expected 1 test error for the unbounded write, got %d", len(ft.errors))
	}
}

type fakeT struct {
	errors []string
}

func (f *fakeT) Helper() {}
func (f *fakeT) Errorf(format string, args ...any) {
	f.errors = append(f.errors, format)
}

func mustExec(t *testing.T, db *sql.DB, ctx context.Context, query string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, query); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

func mustQuery(t *testing.T, db *sql.DB, ctx context.Context, query string) {
	t.Helper()
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	_ = rows.Close()
}
