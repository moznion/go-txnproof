// The pgx-native scenarios verify the Session-based integration (pgxtracer.go):
// no database/sql anywhere in the stack — statements are observed through a
// pgx.QueryTracer feeding per-connection txnproof.Sessions — and every verdict
// is cross-checked against the server's own log via pgcheck, exactly like the
// driver-middleware scenarios.
package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moznion/go-txnproof"
	"github.com/moznion/go-txnproof/pgcheck"
)

// pgxHarness mirrors harness for a pgx-native pgxpool handle.
type pgxHarness struct {
	t        *testing.T
	pool     *pgxpool.Pool
	reporter *txnproof.CollectingReporter
	detector *txnproof.Detector
	logPath  string
	checker  *pgcheck.Checker
}

func newPgxHarness(t *testing.T) *pgxHarness {
	t.Helper()
	dsn, logPath := env(t)
	reporter := txnproof.NewCollectingReporter()
	detector := txnproof.New(txnproof.WithReporter(reporter))

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	newPgxTracer(detector).configurePool(cfg)

	ctx := context.Background()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("connect to PostgreSQL: %v", err)
	}

	checker, err := pgcheck.New()
	if err != nil {
		t.Fatal(err)
	}
	h := &pgxHarness{t: t, pool: pool, reporter: reporter, detector: detector, logPath: logPath, checker: checker}
	for _, ddl := range []string{
		"CREATE TABLE IF NOT EXISTS e2e_users (id bigserial PRIMARY KEY, v int NOT NULL)",
		"CREATE TABLE IF NOT EXISTS e2e_audit (id bigserial PRIMARY KEY, v int NOT NULL)",
	} {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}
	return h
}

// runScenario mirrors harness.runScenario on a dedicated pgx connection.
func (h *pgxHarness) runScenario(name string, fn func(ctx context.Context, conn *pgxpool.Conn) error) string {
	h.t.Helper()
	label := fmt.Sprintf("%s-%d", name, time.Now().UnixNano())
	ctx := context.Background()

	conn, err := h.pool.Acquire(ctx)
	if err != nil {
		h.t.Fatalf("acquire connection: %v", err)
	}
	defer conn.Release()

	// The markers run outside the boundary: client-side they are ignored
	// (no boundary in context), server-side they delimit the scenario.
	if _, err := conn.Exec(ctx, pgcheck.BeginMarker(label)); err != nil {
		h.t.Fatalf("begin marker: %v", err)
	}
	if err := h.detector.InBoundary(ctx, label, func(ctx context.Context) error {
		return fn(ctx, conn)
	}); err != nil {
		h.t.Fatalf("scenario %s: %v", name, err)
	}
	if _, err := conn.Exec(ctx, pgcheck.EndMarker(label)); err != nil {
		h.t.Fatalf("end marker: %v", err)
	}
	return label
}

// Scenario (pgx-a): two auto-commit writes through the native path must be
// flagged by both observers — proves statements flow tracer → Session at all.
func TestPgxNativeTwoAutoCommitWritesViolate(t *testing.T) {
	h := newPgxHarness(t)

	label := h.runScenario("pgx-two-auto-commit", func(ctx context.Context, conn *pgxpool.Conn) error {
		if _, err := conn.Exec(ctx, "INSERT INTO e2e_users (v) VALUES (20)"); err != nil {
			return err
		}
		_, err := conn.Exec(ctx, "INSERT INTO e2e_audit (v) VALUES (20)")
		return err
	})

	v := clientViolationFor(t, h.reporter, label)
	if v == nil {
		t.Fatal("txnproof: expected a violation for two auto-commit writes")
	}
	if v.WriteUnits != 2 || writeCount(v) != 2 {
		t.Errorf("txnproof: expected 2 units / 2 writes, got units=%d writes=%d", v.WriteUnits, writeCount(v))
	}

	rep := serverReportFor(t, h.checker, h.logPath, label)
	if rep.Atomic() || rep.WriteUnits != 2 {
		t.Errorf("pgcheck: expected 2 server-side units, got %s", rep)
	}
}

// Scenario (pgx-b): pgx's Begin()/Commit() surface as textual
// "begin"/"commit" through the tracer, so the writes inside share one unit —
// no BeginTx plumbing needed for explicit transactions.
func TestPgxNativeTransactionIsClean(t *testing.T) {
	h := newPgxHarness(t)

	label := h.runScenario("pgx-single-tx", func(ctx context.Context, conn *pgxpool.Conn) error {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, "INSERT INTO e2e_users (v) VALUES (21)"); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "INSERT INTO e2e_audit (v) VALUES (21)"); err != nil {
			return err
		}
		return tx.Commit(ctx)
	})

	if v := clientViolationFor(t, h.reporter, label); v != nil {
		t.Errorf("txnproof: expected no violation for writes inside pool.Begin/Commit, got %s", v)
	}

	rep := serverReportFor(t, h.checker, h.logPath, label)
	if !rep.Atomic() || rep.WriteUnits != 1 {
		t.Errorf("pgcheck: expected 1 server-side unit, got %s", rep)
	}
	if len(rep.Units) == 1 && len(rep.Units[0].Statements) != 2 {
		t.Errorf("pgcheck: expected both writes in the unit, got %s", rep)
	}
}

// Scenario (pgx-c): a batch outside any transaction is pipelined up to one
// Sync, so the server runs it as ONE implicit transaction — the tracer
// brackets it with BeginTx/EndTx and both observers must agree on 1 unit.
// Without the bracketing, the client would report 2 auto-commit units and
// disagree with the server: this scenario pins the batch semantics.
func TestPgxNativeBatchIsOneImplicitTransaction(t *testing.T) {
	h := newPgxHarness(t)

	label := h.runScenario("pgx-batch-implicit", func(ctx context.Context, conn *pgxpool.Conn) error {
		var b pgx.Batch
		b.Queue("INSERT INTO e2e_users (v) VALUES (22)")
		b.Queue("INSERT INTO e2e_audit (v) VALUES (22)")
		return conn.SendBatch(ctx, &b).Close()
	})

	if v := clientViolationFor(t, h.reporter, label); v != nil {
		t.Errorf("txnproof: expected no violation for one batch (one implicit transaction), got %s", v)
	}

	rep := serverReportFor(t, h.checker, h.logPath, label)
	if !rep.Atomic() || rep.WriteUnits != 1 {
		t.Errorf("pgcheck: expected 1 server-side unit for the batch, got %s", rep)
	}
}

// Scenario (pgx-d): a batch inside an explicit transaction belongs to that
// transaction — the tracer must NOT bracket it (the connection is not idle),
// or the outer unit would be split in two and a lone batch write would
// wrongly violate together with a preceding write.
func TestPgxNativeBatchInsideTransactionJoinsIt(t *testing.T) {
	h := newPgxHarness(t)

	label := h.runScenario("pgx-batch-in-tx", func(ctx context.Context, conn *pgxpool.Conn) error {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, "INSERT INTO e2e_users (v) VALUES (23)"); err != nil {
			return err
		}
		var b pgx.Batch
		b.Queue("INSERT INTO e2e_audit (v) VALUES (23)")
		if err := tx.SendBatch(ctx, &b).Close(); err != nil {
			return err
		}
		return tx.Commit(ctx)
	})

	if v := clientViolationFor(t, h.reporter, label); v != nil {
		t.Errorf("txnproof: expected no violation (batch joins the explicit tx), got %s", v)
	}

	rep := serverReportFor(t, h.checker, h.logPath, label)
	if !rep.Atomic() || rep.WriteUnits != 1 {
		t.Errorf("pgcheck: expected 1 server-side unit, got %s", rep)
	}
}

// Scenario (pgx-e): writes through the query path (INSERT ... RETURNING via
// QueryRow) are observed exactly once each, like the driver-middleware path.
func TestPgxNativeQueryPathWritesViolate(t *testing.T) {
	h := newPgxHarness(t)

	label := h.runScenario("pgx-returning-writes", func(ctx context.Context, conn *pgxpool.Conn) error {
		var id int64
		if err := conn.QueryRow(ctx, "INSERT INTO e2e_users (v) VALUES (24) RETURNING id").Scan(&id); err != nil {
			return err
		}
		return conn.QueryRow(ctx, "INSERT INTO e2e_audit (v) VALUES (24) RETURNING id").Scan(&id)
	})

	v := clientViolationFor(t, h.reporter, label)
	if v == nil {
		t.Fatal("txnproof: expected a violation for two auto-commit RETURNING writes")
	}
	if v.WriteUnits != 2 || writeCount(v) != 2 {
		t.Errorf("txnproof: expected 2 units / 2 writes via the query path, got units=%d writes=%d", v.WriteUnits, writeCount(v))
	}

	rep := serverReportFor(t, h.checker, h.logPath, label)
	if rep.WriteUnits != 2 {
		t.Errorf("pgcheck: expected 2 server-side units, got %s", rep)
	}
}
