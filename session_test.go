package txnproof

import (
	"context"
	"testing"
)

// The Session tests drive the exported native-driver observation surface
// directly — no driver, no database — and assert it applies the same
// counting rules as the driver middleware (which is built on it).

func TestSessionSplitAutoCommitWritesViolate(t *testing.T) {
	reporter := NewCollectingReporter()
	det := New(WithReporter(reporter))
	s := det.NewSession()

	ctx, b := det.StartBoundary(context.Background(), "session-split")
	s.Observe(ctx, "INSERT INTO users (v) VALUES (1)")
	s.Observe(ctx, "SELECT count(*) FROM users")
	s.Observe(ctx, "INSERT INTO audit (v) VALUES (1)")
	b.Finish()

	vs := reporter.Violations()
	if len(vs) != 1 {
		t.Fatalf("expected 1 violation, got %d: %+v", len(vs), vs)
	}
	if vs[0].Boundary != "session-split" || vs[0].WriteUnits != 2 {
		t.Errorf("expected boundary session-split with 2 units, got %s", vs[0])
	}
}

func TestSessionTextualTransactionFormsOneUnit(t *testing.T) {
	reporter := NewCollectingReporter()
	det := New(WithReporter(reporter))
	s := det.NewSession()

	ctx, b := det.StartBoundary(context.Background(), "session-textual-tx")
	s.Observe(ctx, "BEGIN")
	s.Observe(ctx, "INSERT INTO users (v) VALUES (1)")
	s.Observe(ctx, "INSERT INTO audit (v) VALUES (1)")
	s.Observe(ctx, "COMMIT")
	b.Finish()

	reporter.RequireNoViolations(t)
}

// ROLLBACK TO SAVEPOINT must not end the transaction: were it mistaken for a
// transaction end, the write after it would look auto-committed and violate.
func TestSessionSavepointRollbackKeepsTransactionOpen(t *testing.T) {
	reporter := NewCollectingReporter()
	det := New(WithReporter(reporter))
	s := det.NewSession()

	ctx, b := det.StartBoundary(context.Background(), "session-savepoint")
	s.Observe(ctx, "BEGIN")
	s.Observe(ctx, "INSERT INTO users (v) VALUES (1)")
	s.Observe(ctx, "SAVEPOINT sp")
	s.Observe(ctx, "INSERT INTO audit (v) VALUES (1)")
	s.Observe(ctx, "ROLLBACK TO SAVEPOINT sp")
	s.Observe(ctx, "INSERT INTO audit (v) VALUES (2)")
	s.Observe(ctx, "COMMIT")
	b.Finish()

	reporter.RequireNoViolations(t)
}

// A rolled-back transaction still counts as a unit (a partial-write path
// structurally exists), exactly like the driver middleware.
func TestSessionRolledBackTransactionStillCounts(t *testing.T) {
	reporter := NewCollectingReporter()
	det := New(WithReporter(reporter))
	s := det.NewSession()

	ctx, b := det.StartBoundary(context.Background(), "session-rollback")
	s.Observe(ctx, "BEGIN")
	s.Observe(ctx, "INSERT INTO users (v) VALUES (1)")
	s.Observe(ctx, "ROLLBACK")
	s.Observe(ctx, "INSERT INTO audit (v) VALUES (1)")
	b.Finish()

	vs := reporter.Violations()
	if len(vs) != 1 || vs[0].WriteUnits != 2 {
		t.Fatalf("expected 1 violation with 2 units (rolled-back tx counts), got %+v", vs)
	}
}

// BeginTx/EndTx bracket transactions that never surface as statement text
// (driver-API transactions, pgx batch pipelines): the writes in between share
// one unit.
func TestSessionBeginTxEndTxFormOneUnit(t *testing.T) {
	reporter := NewCollectingReporter()
	det := New(WithReporter(reporter))
	s := det.NewSession()

	ctx, b := det.StartBoundary(context.Background(), "session-api-tx")
	s.BeginTx()
	s.Observe(ctx, "INSERT INTO users (v) VALUES (1)")
	s.Observe(ctx, "INSERT INTO audit (v) VALUES (1)")
	s.EndTx()
	b.Finish()

	reporter.RequireNoViolations(t)
}

// Two sessions are two connections: their transactions never merge. An
// explicit transaction on one and an auto-commit write on the other are two
// units even though the statements interleave in time.
func TestSessionsAttributeIndependently(t *testing.T) {
	reporter := NewCollectingReporter()
	det := New(WithReporter(reporter))
	s1 := det.NewSession()
	s2 := det.NewSession()

	ctx, b := det.StartBoundary(context.Background(), "session-two-conns")
	s1.Observe(ctx, "BEGIN")
	s1.Observe(ctx, "INSERT INTO users (v) VALUES (1)")
	s2.Observe(ctx, "INSERT INTO audit (v) VALUES (1)")
	s1.Observe(ctx, "COMMIT")
	b.Finish()

	vs := reporter.Violations()
	if len(vs) != 1 || vs[0].WriteUnits != 2 {
		t.Fatalf("expected 1 violation with 2 units (one per connection), got %+v", vs)
	}
}

// Statements observed with no boundary in context are ignored, and reported
// as unbounded writes only when that detection is opted into — the same
// contract as the driver middleware.
func TestSessionOutsideBoundary(t *testing.T) {
	t.Run("ignored by default", func(t *testing.T) {
		reporter := NewCollectingReporter()
		det := New(WithReporter(reporter))
		s := det.NewSession()

		s.Observe(context.Background(), "INSERT INTO users (v) VALUES (1)")
		s.Observe(context.Background(), "INSERT INTO audit (v) VALUES (1)")

		reporter.RequireNoViolations(t)
		reporter.RequireNoUnboundedWrites(t)
	})

	t.Run("reported with unbounded-write detection", func(t *testing.T) {
		reporter := NewCollectingReporter()
		det := New(WithReporter(reporter), WithUnboundedWriteDetection())
		s := det.NewSession()

		s.Observe(context.Background(), "INSERT INTO users (v) VALUES (1)")

		if got := reporter.UnboundedWrites(); len(got) != 1 {
			t.Fatalf("expected 1 unbounded write, got %+v", got)
		}
	})
}
