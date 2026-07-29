package txnproof

import (
	"context"
	"database/sql"
	"slices"
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
	// Nothing allowed this boundary, so the report carries no declined counts
	// and must not mention any.
	if got := vs[0].AllowedWriteUnits; got != nil {
		t.Errorf("AllowedWriteUnits = %v for an unmarked boundary, want nil", got)
	}
	if strings.Contains(vs[0].String(), "allowed for") {
		t.Errorf("String() should not mention an allow: %s", vs[0].String())
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

func TestAllowlistExactUnitsSuppressesOnlyThatCount(t *testing.T) {
	// The central list decides exactly like an in-code AllowNonAtomic mark with
	// the same counts: a covered count is suppressed, any other one is an
	// unreviewed violation — and leaves the entry unused, so CI notices too.
	t.Run("matching count is suppressed", func(t *testing.T) {
		al := NewAllowlist().Add("BestEffortAudit", "domain write plus audit write (TICKET-123)", 2)
		det, cr, db := setup(t, WithAllowlist(al))
		runWritingBoundary(t, det, db, 2)

		if vs := cr.Violations(); len(vs) != 0 {
			t.Fatalf("2 write units are exactly what the entry covers, got %+v", vs)
		}
		if unused := al.UnusedEntries(); len(unused) != 0 {
			t.Fatalf("the entry suppressed a violation, got unused %v", unused)
		}
	})

	t.Run("uncovered count violates and leaves the entry unused", func(t *testing.T) {
		al := NewAllowlist().Add("BestEffortAudit", "domain write plus audit write (TICKET-123)", 2)
		det, cr, db := setup(t, WithAllowlist(al))
		runWritingBoundary(t, det, db, 3)

		vs := cr.Violations()
		if len(vs) != 1 || vs[0].Boundary != "BestEffortAudit" || vs[0].WriteUnits != 3 {
			t.Fatalf("a count outside the entry must be reported, got %+v", vs)
		}
		if got := vs[0].AllowedWriteUnits; len(got) != 1 || got[0] != 2 {
			t.Fatalf("AllowedWriteUnits = %v, want [2] (the entry's counts)", got)
		}
		unused := al.UnusedEntries()
		if len(unused) != 1 || unused[0] != "BestEffortAudit" {
			t.Fatalf("an entry that suppressed nothing must show up as unused, got %v", unused)
		}
	})

	t.Run("several counts allow each", func(t *testing.T) {
		for _, writes := range []int{2, 3} {
			al := NewAllowlist().Add("BestEffortAudit", "two on the fast path, three on a cache miss", 2, 3)
			det, cr, db := setup(t, WithAllowlist(al))
			runWritingBoundary(t, det, db, writes)
			if vs := cr.Violations(); len(vs) != 0 {
				t.Fatalf("%d write units are covered by the entry, got %+v", writes, vs)
			}
		}
	})

	t.Run("caller slice is copied", func(t *testing.T) {
		counts := []int{2}
		al := NewAllowlist().Add("BestEffortAudit", "reviewed for exactly 2 units", counts...)
		counts[0] = 3 // must not change what the entry allows

		det, cr, db := setup(t, WithAllowlist(al))
		runWritingBoundary(t, det, db, 2)
		if vs := cr.Violations(); len(vs) != 0 {
			t.Fatalf("the entry was added for 2 units, got %+v", vs)
		}
	})
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

// runWritingBoundary runs the given number of auto-commit writes (one write
// unit each) inside a boundary carrying opts, and finishes it.
func runWritingBoundary(t *testing.T, det *Detector, db *sql.DB, writes int, opts ...BoundaryOption) {
	t.Helper()
	ctx, finish := det.StartBoundary(context.Background(), "BestEffortAudit", opts...)
	for i := 0; i < writes; i++ {
		mustExec(t, db, ctx, "INSERT INTO a (id) VALUES (1)")
	}
	finish.Finish()
}

func TestAllowNonAtomicExactUnitsSuppressesOnlyThatCount(t *testing.T) {
	const reason = "audit write plus the domain write, nothing more (TICKET-123)"

	t.Run("matching count is suppressed", func(t *testing.T) {
		det, cr, db := setup(t)
		runWritingBoundary(t, det, db, 2, AllowNonAtomic(reason, 2))

		if vs := cr.Violations(); len(vs) != 0 {
			t.Fatalf("2 write units are exactly what the allow covers, got %+v", vs)
		}
		if sa := cr.StaleAllows(); len(sa) != 0 {
			t.Fatalf("the allow suppressed a violation; must not be stale, got %+v", sa)
		}
	})

	t.Run("larger count violates", func(t *testing.T) {
		det, cr, db := setup(t)
		runWritingBoundary(t, det, db, 3, AllowNonAtomic(reason, 2))

		vs := cr.Violations()
		if len(vs) != 1 || vs[0].Boundary != "BestEffortAudit" || vs[0].WriteUnits != 3 {
			t.Fatalf("a count outside the allow must be reported, got %+v", vs)
		}
		// The report must say a mark exists and why it did not apply, or it
		// reads as an ordinary violation of an unmarked boundary.
		if got := vs[0].AllowedWriteUnits; len(got) != 1 || got[0] != 2 {
			t.Fatalf("AllowedWriteUnits = %v, want [2]", got)
		}
		if s := vs[0].String(); !strings.Contains(s, "allowed for exactly 2 write unit(s)") {
			t.Errorf("String() should explain the declined allow: %s", s)
		}
		if sa := cr.StaleAllows(); len(sa) != 0 {
			t.Fatalf("the violation is the signal; no stale allow expected, got %+v", sa)
		}
	})

	t.Run("smaller violating count violates", func(t *testing.T) {
		det, cr, db := setup(t)
		runWritingBoundary(t, det, db, 2, AllowNonAtomic(reason, 3))

		vs := cr.Violations()
		if len(vs) != 1 || vs[0].WriteUnits != 2 {
			t.Fatalf("expected 1 violation with 2 units, got %+v", vs)
		}
	})

	t.Run("non-violating count is stale, not a violation", func(t *testing.T) {
		det, cr, db := setup(t)
		runWritingBoundary(t, det, db, 1, AllowNonAtomic(reason, 3))

		if vs := cr.Violations(); len(vs) != 0 {
			t.Fatalf("a single write unit is atomic, got %+v", vs)
		}
		sa := cr.StaleAllows()
		if len(sa) != 1 || sa[0].WriteUnits != 1 || sa[0].Reason != reason {
			t.Fatalf("expected the allow to be reported stale, got %+v", sa)
		}
	})
}

func TestAllowNonAtomicSeveralExactUnitsAllowEach(t *testing.T) {
	// A boundary whose write count differs per code path can list every
	// reviewed count; anything else still violates.
	opt := AllowNonAtomic("two on the fast path, three when the cache misses (TICKET-123)", 2, 3)

	for _, writes := range []int{2, 3} {
		det, cr, db := setup(t)
		runWritingBoundary(t, det, db, writes, opt)
		if vs := cr.Violations(); len(vs) != 0 {
			t.Fatalf("%d write units are covered by the allow, got %+v", writes, vs)
		}
	}

	det, cr, db := setup(t)
	runWritingBoundary(t, det, db, 4, opt)
	vs := cr.Violations()
	if len(vs) != 1 || vs[0].WriteUnits != 4 {
		t.Fatalf("4 write units are not covered; expected 1 violation, got %+v", vs)
	}
	if s := vs[0].String(); !strings.Contains(s, "allowed for exactly 2 or 3 write unit(s)") {
		t.Errorf("String() should list every allowed count: %s", s)
	}
}

func TestAllowNonAtomicExactUnitsFallsThroughToAllowlist(t *testing.T) {
	// The in-code mark wins first; when its exact count does not cover this
	// execution, the central Allowlist still gets its say (and is marked used).
	al := NewAllowlist().Add("BestEffortAudit", "audit writes are best-effort (TICKET-123)")
	det, cr, db := setup(t, WithAllowlist(al))

	runWritingBoundary(t, det, db, 3, AllowNonAtomic("reviewed for exactly 2 units", 2))

	if vs := cr.Violations(); len(vs) != 0 {
		t.Fatalf("the allowlist entry should suppress the fallen-through violation, got %+v", vs)
	}
	if unused := al.UnusedEntries(); len(unused) != 0 {
		t.Fatalf("the allowlist entry actually suppressed a violation, got unused %v", unused)
	}
}

func TestAllowNonAtomicExactUnitsCountsTransactionsAndAutoCommits(t *testing.T) {
	// A write unit is a transaction that wrote or an auto-commit write: two
	// writes sharing one transaction plus one auto-commit write are 2 units.
	det, cr, _ := setup(t)

	ctx, b := det.StartBoundary(context.Background(), "BestEffortAudit",
		AllowNonAtomic("one transaction plus the audit write (TICKET-123)", 2))
	det.record(ctx, "INSERT INTO t VALUES (1)", KindWrite, 1)
	det.record(ctx, "UPDATE t SET a = 1", KindWrite, 1)
	det.record(ctx, "INSERT INTO audit VALUES (1)", KindWrite, 0)
	b.Finish()

	if vs := cr.Violations(); len(vs) != 0 {
		t.Fatalf("expected the 2 write units to be covered, got %+v", vs)
	}
	if sa := cr.StaleAllows(); len(sa) != 0 {
		t.Fatalf("the allow suppressed a violation; must not be stale, got %+v", sa)
	}
}

func TestAllowNonAtomicExactUnitsCallerSliceIsCopied(t *testing.T) {
	counts := []int{2}
	opt := AllowNonAtomic("reviewed for exactly 2 units (TICKET-123)", counts...)
	counts[0] = 3 // must not change what the option allows

	det, cr, db := setup(t)
	runWritingBoundary(t, det, db, 2, opt)
	if vs := cr.Violations(); len(vs) != 0 {
		t.Fatalf("the allow was created for 2 units, got %+v", vs)
	}
}

// runWritingBoundaryMarkedHere runs the given number of auto-commit writes
// inside an unmarked boundary and marks it with AllowNonAtomicHere from the
// site of the last write — the in-code alternative to marking at the start.
func runWritingBoundaryMarkedHere(t *testing.T, det *Detector, db *sql.DB, writes int, reason string, units ...int) {
	t.Helper()
	ctx, finish := det.StartBoundary(context.Background(), "BestEffortAudit")
	if writes == 0 {
		AllowNonAtomicHere(ctx, reason, units...)
	}
	for i := 0; i < writes; i++ {
		if i == writes-1 {
			AllowNonAtomicHere(ctx, reason, units...)
		}
		mustExec(t, db, ctx, "INSERT INTO a (id) VALUES (1)")
	}
	finish.Finish()
}

func TestAllowNonAtomicHereSuppressesViolation(t *testing.T) {
	det, cr, db := setup(t)

	ctx, finish := det.StartBoundary(context.Background(), "BestEffortAudit")
	mustExec(t, db, ctx, "INSERT INTO a (id) VALUES (1)")
	// Marked where the non-atomicity actually is, not where the boundary started.
	AllowNonAtomicHere(ctx, "audit writes are intentionally best-effort (TICKET-123)")
	mustExec(t, db, ctx, "INSERT INTO audit (id) VALUES (1)")
	finish.Finish()

	if vs := cr.Violations(); len(vs) != 0 {
		t.Fatalf("the mark should suppress the violation, got %+v", vs)
	}
	if sa := cr.StaleAllows(); len(sa) != 0 {
		t.Fatalf("the mark suppressed a violation; must not be stale, got %+v", sa)
	}
}

func TestAllowNonAtomicHereDecidesLikeTheOption(t *testing.T) {
	// The two in-code mechanisms are a choice of where the exemption lives,
	// never of what it can express: for the same reason and counts they must
	// produce byte-identical reports.
	const reason = "audit write plus the domain write, nothing more (TICKET-123)"
	for _, tc := range []struct {
		name   string
		writes int
		units  []int
	}{
		{"unconstrained", 3, nil},
		{"exact count met", 2, []int{2}},
		{"exact count exceeded", 3, []int{2}},
		{"several counts", 3, []int{2, 3}},
		{"atomic execution is stale, never a violation", 1, []int{2}},
		{"count below 2 can never match", 2, []int{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			detOpt, crOpt, dbOpt := setup(t)
			runWritingBoundary(t, detOpt, dbOpt, tc.writes, AllowNonAtomic(reason, tc.units...))

			detHere, crHere, dbHere := setup(t)
			runWritingBoundaryMarkedHere(t, detHere, dbHere, tc.writes, reason, tc.units...)

			vOpt, vHere := crOpt.Violations(), crHere.Violations()
			if len(vOpt) != len(vHere) {
				t.Fatalf("got %d violation(s) from the mark, want %d as with the option (%+v vs %+v)", len(vHere), len(vOpt), vHere, vOpt)
			}
			for i := range vOpt {
				if vHere[i].WriteUnits != vOpt[i].WriteUnits {
					t.Errorf("violation WriteUnits = %d, want %d as with the option", vHere[i].WriteUnits, vOpt[i].WriteUnits)
				}
				if !slices.Equal(vHere[i].AllowedWriteUnits, vOpt[i].AllowedWriteUnits) {
					t.Errorf("violation AllowedWriteUnits = %v, want %v as with the option", vHere[i].AllowedWriteUnits, vOpt[i].AllowedWriteUnits)
				}
			}

			sOpt, sHere := crOpt.StaleAllows(), crHere.StaleAllows()
			if len(sOpt) != len(sHere) {
				t.Fatalf("got %d stale allow(s) from the mark, want %d as with the option (%+v vs %+v)", len(sHere), len(sOpt), sHere, sOpt)
			}
			for i := range sOpt {
				if sHere[i] != sOpt[i] {
					t.Errorf("stale allow = %+v, want %+v as with the option", sHere[i], sOpt[i])
				}
			}
		})
	}
}

func TestAllowNonAtomicHereFallsThroughToAllowlist(t *testing.T) {
	// Same fall-through as the option: a count the mark does not cover leaves
	// the central Allowlist to decide.
	al := NewAllowlist().Add("BestEffortAudit", "audit writes are best-effort (TICKET-123)")
	det, cr, db := setup(t, WithAllowlist(al))

	runWritingBoundaryMarkedHere(t, det, db, 3, "reviewed for exactly 2 units", 2)

	if vs := cr.Violations(); len(vs) != 0 {
		t.Fatalf("the allowlist entry should suppress the fallen-through violation, got %+v", vs)
	}
	if unused := al.UnusedEntries(); len(unused) != 0 {
		t.Fatalf("the allowlist entry actually suppressed a violation, got unused %v", unused)
	}
}

func TestAllowNonAtomicHereLastMarkWins(t *testing.T) {
	// A mark at the write site is the more specific claim and replaces one made
	// at boundary start, counts included.
	det, cr, db := setup(t)

	ctx, finish := det.StartBoundary(context.Background(), "BestEffortAudit",
		AllowNonAtomic("reviewed for exactly 2 units", 2))
	for i := 0; i < 3; i++ {
		mustExec(t, db, ctx, "INSERT INTO a (id) VALUES (1)")
	}
	AllowNonAtomicHere(ctx, "the retry path writes a third time (TICKET-123)", 3)
	finish.Finish()

	if vs := cr.Violations(); len(vs) != 0 {
		t.Fatalf("the later mark covers 3 units, got %+v", vs)
	}
}

func TestAllowNonAtomicHereMarksInnermostBoundary(t *testing.T) {
	// Consistent with shadowing: the mark lands on the boundary the statements
	// attribute to, and leaves the outer one untouched.
	det, cr, db := setup(t)

	outerCtx, outer := det.StartBoundary(context.Background(), "Outer")
	innerCtx, inner := det.StartBoundary(outerCtx, "Inner")
	AllowNonAtomicHere(innerCtx, "audit writes are best-effort (TICKET-123)")
	mustExec(t, db, innerCtx, "INSERT INTO a (id) VALUES (1)")
	mustExec(t, db, innerCtx, "INSERT INTO audit (id) VALUES (1)")
	inner.Finish()

	mustExec(t, db, outerCtx, "INSERT INTO b (id) VALUES (1)")
	mustExec(t, db, outerCtx, "INSERT INTO c (id) VALUES (1)")
	outer.Finish()

	vs := cr.Violations()
	if len(vs) != 1 || vs[0].Boundary != "Outer" {
		t.Fatalf("expected only the outer boundary to violate, got %+v", vs)
	}
}

func TestAllowNonAtomicHereWithoutBoundaryIsNoOp(t *testing.T) {
	det, cr, db := setup(t)

	// No boundary in this context: the mark must do nothing, not panic, and not
	// leak into the next boundary.
	AllowNonAtomicHere(context.Background(), "no boundary here")

	runWritingBoundary(t, det, db, 2)
	if vs := cr.Violations(); len(vs) != 1 {
		t.Fatalf("expected the unmarked boundary to violate, got %+v", vs)
	}
	if sa := cr.StaleAllows(); len(sa) != 0 {
		t.Fatalf("nothing was marked, got %+v", sa)
	}
}

func TestAllowNonAtomicHereAfterFinishIsIgnored(t *testing.T) {
	det, cr, db := setup(t)

	ctx, finish := det.StartBoundary(context.Background(), "BestEffortAudit")
	mustExec(t, db, ctx, "INSERT INTO a (id) VALUES (1)")
	mustExec(t, db, ctx, "INSERT INTO audit (id) VALUES (1)")
	finish.Finish()

	// The boundary is already evaluated and reported; a late mark (e.g. from a
	// goroutine outliving the request) must not rewrite history.
	AllowNonAtomicHere(ctx, "too late (TICKET-123)")
	finish.Finish()

	if vs := cr.Violations(); len(vs) != 1 {
		t.Fatalf("expected exactly the one violation reported at Finish, got %+v", vs)
	}
}

func TestAllowNonAtomicHereCallerSliceIsCopied(t *testing.T) {
	det, cr, db := setup(t)

	counts := []int{2}
	ctx, finish := det.StartBoundary(context.Background(), "BestEffortAudit")
	AllowNonAtomicHere(ctx, "reviewed for exactly 2 units (TICKET-123)", counts...)
	counts[0] = 3 // must not change what the mark allows
	mustExec(t, db, ctx, "INSERT INTO a (id) VALUES (1)")
	mustExec(t, db, ctx, "INSERT INTO audit (id) VALUES (1)")
	finish.Finish()

	if vs := cr.Violations(); len(vs) != 0 {
		t.Fatalf("the mark was made for 2 units, got %+v", vs)
	}
}

func TestAllowNonAtomicHereConcurrentWithWrites(t *testing.T) {
	// The mark reaches a live boundary from any goroutine, unlike the option;
	// run under -race to prove the bookkeeping stays sound.
	det, cr, db := setup(t)

	ctx, finish := det.StartBoundary(context.Background(), "BestEffortAudit")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			AllowNonAtomicHere(ctx, "audit writes are best-effort (TICKET-123)")
			mustExec(t, db, ctx, "INSERT INTO a (id) VALUES (1)")
		}()
	}
	wg.Wait()
	finish.Finish()

	if vs := cr.Violations(); len(vs) != 0 {
		t.Fatalf("the boundary is marked, got %+v", vs)
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
