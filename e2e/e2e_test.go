// Package e2e self-verifies txnproof against a real PostgreSQL server: every
// scenario is observed twice — client-side by txnproof (a CollectingReporter
// on the wrapped pgx driver) and server-side by pgcheck (parsing the
// server's own log) — and the two verdicts must agree.
//
// The tests need a running PostgreSQL whose stderr-format log is readable
// and configured as pgcheck documents (log_line_prefix = '%m [%p] %q%x %v ',
// log_statement = 'all', lc_messages = 'C'). They skip unless both
// environment variables are set:
//
//	TXNPROOF_E2E_PG_DSN  connection string for the server
//	TXNPROOF_E2E_PG_LOG  path to the server's current log file
//
// run.sh spins up a throwaway cluster with exactly that configuration and
// runs the tests against it.
package e2e

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	txnproof "github.com/moznion/go-txnproof"
	"github.com/moznion/go-txnproof/crosscheck"
	"github.com/moznion/go-txnproof/pgcheck"
)

// harness wires one detector-wrapped database handle to one test, plus the
// pgcheck side of the cross-check.
type harness struct {
	t        *testing.T
	db       *sql.DB
	reporter *txnproof.CollectingReporter
	detector *txnproof.Detector
	logPath  string
	checker  *pgcheck.Checker
}

// env returns the e2e configuration, skipping the test when it is absent.
func env(t *testing.T) (dsn, logPath string) {
	t.Helper()
	dsn = os.Getenv("TXNPROOF_E2E_PG_DSN")
	logPath = os.Getenv("TXNPROOF_E2E_PG_LOG")
	if dsn == "" || logPath == "" {
		t.Skip("set TXNPROOF_E2E_PG_DSN and TXNPROOF_E2E_PG_LOG to run the e2e tests (see run.sh)")
	}
	return dsn, logPath
}

// newHarness opens the database through detector.WrapConnector. One test
// covers detector.Wrap + sql.Register instead (TestTwoAutoCommitWritesViolate).
func newHarness(t *testing.T) *harness {
	t.Helper()
	dsn, logPath := env(t)
	reporter := txnproof.NewCollectingReporter()
	detector := txnproof.New(txnproof.WithReporter(reporter))

	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	db := sql.OpenDB(detector.WrapConnector(stdlib.GetConnector(*cfg)))
	return finishHarness(t, db, reporter, detector, logPath)
}

func finishHarness(t *testing.T, db *sql.DB, reporter *txnproof.CollectingReporter, detector *txnproof.Detector, logPath string) *harness {
	t.Helper()
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Ping(); err != nil {
		t.Fatalf("connect to PostgreSQL: %v", err)
	}
	checker, err := pgcheck.New()
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, db: db, reporter: reporter, detector: detector, logPath: logPath, checker: checker}
	h.setupTables()
	return h
}

func (h *harness) setupTables() {
	h.t.Helper()
	for _, ddl := range []string{
		"CREATE TABLE IF NOT EXISTS e2e_users (id bigserial PRIMARY KEY, v int NOT NULL)",
		"CREATE TABLE IF NOT EXISTS e2e_audit (id bigserial PRIMARY KEY, v int NOT NULL)",
	} {
		if _, err := h.db.ExecContext(context.Background(), ddl); err != nil {
			h.t.Fatalf("setup: %v", err)
		}
	}
}

// runScenario executes fn on a dedicated connection, inside a txnproof
// boundary, delimited by pgcheck markers in the server log. The returned
// label names both the boundary and the log markers; it carries a nonce so
// reruns against a lived-in log file cannot match a previous run's markers.
func (h *harness) runScenario(name string, fn func(ctx context.Context, conn *sql.Conn) error) string {
	h.t.Helper()
	label := fmt.Sprintf("%s-%d", name, time.Now().UnixNano())
	ctx := context.Background()

	conn, err := h.db.Conn(ctx)
	if err != nil {
		h.t.Fatalf("acquire connection: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// The markers run outside the boundary: client-side they are ignored
	// (no boundary in context), server-side they delimit the scenario.
	if _, err := conn.ExecContext(ctx, pgcheck.BeginMarker(label)); err != nil {
		h.t.Fatalf("begin marker: %v", err)
	}
	if err := h.detector.InBoundary(ctx, label, func(ctx context.Context) error {
		return fn(ctx, conn)
	}); err != nil {
		h.t.Fatalf("scenario %s: %v", name, err)
	}
	if _, err := conn.ExecContext(ctx, pgcheck.EndMarker(label)); err != nil {
		h.t.Fatalf("end marker: %v", err)
	}
	return label
}

// clientViolation returns txnproof's violation for the boundary, or nil.
func (h *harness) clientViolation(label string) *txnproof.Violation {
	h.t.Helper()
	var found *txnproof.Violation
	for _, v := range h.reporter.Violations() {
		if v.Boundary != label {
			continue
		}
		if found != nil {
			h.t.Fatalf("boundary %q reported more than one violation", label)
		}
		v := v
		found = &v
	}
	return found
}

// serverReport runs pgcheck over the server log for the scenario. The log
// file is written by the server asynchronously, so it retries until the
// markers show up (or the deadline passes). A non-atomic scenario is not an
// error here: the *crosscheck.NonAtomicError verdict is folded into the
// returned Report, which the caller asserts on.
func (h *harness) serverReport(label string) *crosscheck.Report {
	h.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		f, err := os.Open(h.logPath)
		if err != nil {
			h.t.Fatalf("open server log: %v", err)
		}
		rep, err := h.checker.VerifyScenario(f, label)
		_ = f.Close()

		var nae *crosscheck.NonAtomicError
		switch {
		case err == nil || errors.As(err, &nae):
			return rep
		case strings.Contains(err.Error(), "marker") && time.Now().Before(deadline):
			// The server has not flushed the scenario's lines yet.
			time.Sleep(50 * time.Millisecond)
		default:
			h.t.Fatalf("pgcheck verdict for %q: %v", label, err)
		}
	}
}

// writeCount counts the KindWrite statements txnproof recorded in a violation.
func writeCount(v *txnproof.Violation) int {
	n := 0
	for _, s := range v.Statements {
		if s.Kind == txnproof.KindWrite {
			n++
		}
	}
	return n
}

// Scenario (a): two auto-commit writes must be flagged by both observers.
// This test also covers the detector.Wrap + sql.Register wiring.
func TestTwoAutoCommitWritesViolate(t *testing.T) {
	dsn, logPath := env(t)
	reporter := txnproof.NewCollectingReporter()
	detector := txnproof.New(txnproof.WithReporter(reporter))
	sql.Register("pgx-txnproof-e2e", detector.Wrap(stdlib.GetDefaultDriver()))
	db, err := sql.Open("pgx-txnproof-e2e", dsn)
	if err != nil {
		t.Fatal(err)
	}
	h := finishHarness(t, db, reporter, detector, logPath)

	label := h.runScenario("two-auto-commit", func(ctx context.Context, conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, "INSERT INTO e2e_users (v) VALUES (1)"); err != nil {
			return err
		}
		_, err := conn.ExecContext(ctx, "INSERT INTO e2e_audit (v) VALUES (1)")
		return err
	})

	v := h.clientViolation(label)
	if v == nil {
		t.Fatal("txnproof: expected a violation for two auto-commit writes")
	}
	if v.WriteUnits != 2 || writeCount(v) != 2 {
		t.Errorf("txnproof: expected 2 units / 2 writes, got units=%d writes=%d", v.WriteUnits, writeCount(v))
	}

	rep := h.serverReport(label)
	if rep.Atomic() || rep.WriteUnits != 2 {
		t.Errorf("pgcheck: expected 2 server-side units, got %s", rep)
	}
}

// Scenario (b): the same writes inside one sql.Tx are clean for both.
func TestSingleTransactionIsClean(t *testing.T) {
	h := newHarness(t)

	label := h.runScenario("single-tx", func(ctx context.Context, conn *sql.Conn) error {
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO e2e_users (v) VALUES (2)"); err != nil {
			_ = tx.Rollback()
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO e2e_audit (v) VALUES (2)"); err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	})

	if v := h.clientViolation(label); v != nil {
		t.Errorf("txnproof: expected no violation, got %s", v)
	}

	rep := h.serverReport(label)
	if !rep.Atomic() || rep.WriteUnits != 1 {
		t.Errorf("pgcheck: expected 1 server-side unit, got %s", rep)
	}
	if len(rep.Units) == 1 && len(rep.Units[0].Statements) != 2 {
		t.Errorf("pgcheck: expected both writes in the unit, got %s", rep)
	}
}

// Scenario (c): a rolled-back transaction still counts as a unit, so a
// following auto-commit write makes 2 for both observers.
func TestRolledBackTxThenAutoCommitWriteViolates(t *testing.T) {
	h := newHarness(t)

	label := h.runScenario("rollback-then-write", func(ctx context.Context, conn *sql.Conn) error {
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO e2e_users (v) VALUES (3)"); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Rollback(); err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, "INSERT INTO e2e_audit (v) VALUES (3)")
		return err
	})

	v := h.clientViolation(label)
	if v == nil {
		t.Fatal("txnproof: expected a violation (rolled-back tx still counts)")
	}
	if v.WriteUnits != 2 {
		t.Errorf("txnproof: expected 2 units, got %d", v.WriteUnits)
	}

	rep := h.serverReport(label)
	if rep.WriteUnits != 2 {
		t.Errorf("pgcheck: expected 2 server-side units, got %s", rep)
	}
}

// Scenario (d): textual BEGIN/COMMIT executed as plain statements form one
// transaction for both observers — txnproof tracks them best-effort, and the
// server groups the writes under one transaction anyway.
func TestTextualBeginCommitCountsAsOneUnit(t *testing.T) {
	h := newHarness(t)

	label := h.runScenario("textual-begin-commit", func(ctx context.Context, conn *sql.Conn) error {
		for _, q := range []string{
			"BEGIN",
			"INSERT INTO e2e_users (v) VALUES (4)",
			"INSERT INTO e2e_audit (v) VALUES (4)",
			"COMMIT",
		} {
			if _, err := conn.ExecContext(ctx, q); err != nil {
				return err
			}
		}
		return nil
	})

	// Were the textual BEGIN not tracked, the two writes would look like two
	// auto-commit units and violate — so no violation proves one unit.
	if v := h.clientViolation(label); v != nil {
		t.Errorf("txnproof: expected no violation for writes inside textual BEGIN/COMMIT, got %s", v)
	}

	rep := h.serverReport(label)
	if !rep.Atomic() || rep.WriteUnits != 1 {
		t.Errorf("pgcheck: expected 1 server-side unit, got %s", rep)
	}
	if len(rep.Units) == 1 && len(rep.Units[0].Statements) != 2 {
		t.Errorf("pgcheck: expected both writes in the unit, got %s", rep)
	}
}

// Scenario (e): the prepared-statement path records each execution exactly
// once. Two executions of one prepared INSERT in auto-commit mode must be
// exactly 2 units / 2 recorded writes for txnproof (double-counting would
// inflate both) and exactly 2 single-statement units for pgcheck.
func TestPreparedStatementPathRecordsOnce(t *testing.T) {
	h := newHarness(t)

	label := h.runScenario("prepared-twice", func(ctx context.Context, conn *sql.Conn) error {
		stmt, err := conn.PrepareContext(ctx, "INSERT INTO e2e_users (v) VALUES ($1)")
		if err != nil {
			return err
		}
		defer func() { _ = stmt.Close() }()
		if _, err := stmt.ExecContext(ctx, 5); err != nil {
			return err
		}
		_, err = stmt.ExecContext(ctx, 6)
		return err
	})

	v := h.clientViolation(label)
	if v == nil {
		t.Fatal("txnproof: expected a violation for two auto-commit prepared executions")
	}
	if v.WriteUnits != 2 || writeCount(v) != 2 {
		t.Errorf("txnproof: expected exactly 2 units / 2 recorded writes (no double-counting), got units=%d writes=%d", v.WriteUnits, writeCount(v))
	}

	rep := h.serverReport(label)
	if rep.WriteUnits != 2 {
		t.Errorf("pgcheck: expected 2 server-side units, got %s", rep)
	}
	for _, u := range rep.Units {
		if len(u.Statements) != 1 {
			t.Errorf("pgcheck: expected 1 statement per unit (no double-counting), got %s", rep)
		}
	}
}

// Scenario (f): ROLLBACK TO SAVEPOINT must not end the textual transaction.
// Were the savepoint rollback mistaken for a transaction end, the write after
// it would look auto-committed and the boundary would violate.
func TestSavepointRollbackKeepsTransactionOpen(t *testing.T) {
	h := newHarness(t)

	label := h.runScenario("savepoint-rollback", func(ctx context.Context, conn *sql.Conn) error {
		for _, q := range []string{
			"BEGIN",
			"INSERT INTO e2e_users (v) VALUES (10)",
			"SAVEPOINT sp",
			"INSERT INTO e2e_audit (v) VALUES (10)",
			"ROLLBACK TO SAVEPOINT sp",
			"INSERT INTO e2e_audit (v) VALUES (11)",
			"COMMIT",
		} {
			if _, err := conn.ExecContext(ctx, q); err != nil {
				return err
			}
		}
		return nil
	})

	if v := h.clientViolation(label); v != nil {
		t.Errorf("txnproof: expected no violation (ROLLBACK TO SAVEPOINT must not end the tx), got %s", v)
	}

	rep := h.serverReport(label)
	if !rep.Atomic() || rep.WriteUnits != 1 {
		t.Errorf("pgcheck: expected 1 server-side unit, got %s", rep)
	}
	// The savepoint-rolled-back write still ran inside the transaction.
	if len(rep.Units) == 1 && len(rep.Units[0].Statements) != 3 {
		t.Errorf("pgcheck: expected all 3 writes in the unit, got %s", rep)
	}
}

// Scenario (g): writes executed through the query path (INSERT ... RETURNING
// via QueryRowContext) are recorded exactly once each, like Exec-path writes.
func TestQueryPathWritesViaReturningViolate(t *testing.T) {
	h := newHarness(t)

	label := h.runScenario("returning-writes", func(ctx context.Context, conn *sql.Conn) error {
		var id int64
		if err := conn.QueryRowContext(ctx, "INSERT INTO e2e_users (v) VALUES (12) RETURNING id").Scan(&id); err != nil {
			return err
		}
		return conn.QueryRowContext(ctx, "INSERT INTO e2e_audit (v) VALUES (12) RETURNING id").Scan(&id)
	})

	v := h.clientViolation(label)
	if v == nil {
		t.Fatal("txnproof: expected a violation for two auto-commit RETURNING writes")
	}
	if v.WriteUnits != 2 || writeCount(v) != 2 {
		t.Errorf("txnproof: expected 2 units / 2 writes via the query path, got units=%d writes=%d", v.WriteUnits, writeCount(v))
	}

	rep := h.serverReport(label)
	if rep.WriteUnits != 2 {
		t.Errorf("pgcheck: expected 2 server-side units, got %s", rep)
	}
	for _, u := range rep.Units {
		if len(u.Statements) != 1 {
			t.Errorf("pgcheck: expected 1 statement per unit, got %s", rep)
		}
	}
}

// Scenario (h): reads interleaved with writes never count, on either side —
// the unit count stays at the writes' 2 and the server-side units contain no
// SELECT.
func TestInterleavedReadsDoNotCount(t *testing.T) {
	h := newHarness(t)

	label := h.runScenario("reads-between-writes", func(ctx context.Context, conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, "INSERT INTO e2e_users (v) VALUES (13)"); err != nil {
			return err
		}
		var n int64
		if err := conn.QueryRowContext(ctx, "SELECT count(*) FROM e2e_users").Scan(&n); err != nil {
			return err
		}
		_, err := conn.ExecContext(ctx, "INSERT INTO e2e_audit (v) VALUES (13)")
		return err
	})

	v := h.clientViolation(label)
	if v == nil {
		t.Fatal("txnproof: expected a violation for the two writes around the read")
	}
	if v.WriteUnits != 2 || writeCount(v) != 2 {
		t.Errorf("txnproof: expected 2 units / 2 writes (read must not count), got units=%d writes=%d", v.WriteUnits, writeCount(v))
	}

	rep := h.serverReport(label)
	if rep.WriteUnits != 2 {
		t.Errorf("pgcheck: expected 2 server-side units, got %s", rep)
	}
	for _, u := range rep.Units {
		for _, s := range u.Statements {
			if s.Kind != txnproof.KindWrite {
				t.Errorf("pgcheck: unit contains a non-write statement: %q", s.SQL)
			}
		}
	}
}

// Scenario (i): a write that fails on the server (unique violation) still
// counts as a unit for both observers — txnproof records errored statements
// (only ErrBadConn is skipped), and the server logs the statement before it
// fails. This pins the semantics: a failed write is a structural write path.
func TestFailedWriteStillCountsAsUnit(t *testing.T) {
	h := newHarness(t)

	// Unique per run so reruns against a lived-in cluster don't trip the
	// first (supposed-to-succeed) insert.
	insert := fmt.Sprintf("INSERT INTO e2e_users (id, v) VALUES (%d, 14)", time.Now().UnixNano())
	label := h.runScenario("failed-write", func(ctx context.Context, conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, insert); err != nil {
			return err
		}
		// Deliberate duplicate key; the error is the point, so swallow it.
		if _, err := conn.ExecContext(ctx, insert); err == nil {
			return fmt.Errorf("expected a unique violation, write unexpectedly succeeded")
		}
		return nil
	})

	v := h.clientViolation(label)
	if v == nil {
		t.Fatal("txnproof: expected a violation (failed write still counts as a unit)")
	}
	if v.WriteUnits != 2 || writeCount(v) != 2 {
		t.Errorf("txnproof: expected 2 units / 2 writes incl. the failed one, got units=%d writes=%d", v.WriteUnits, writeCount(v))
	}

	rep := h.serverReport(label)
	if rep.WriteUnits != 2 {
		t.Errorf("pgcheck: expected 2 server-side units incl. the failed write, got %s", rep)
	}
}

// Scenario (j): two boundaries running concurrently on separate connections
// attribute their statements independently — no cross-talk, one violation
// each. Client-side only: the marker-based server correlation assumes a
// quiet database, which deliberately interleaved connections are not.
func TestConcurrentBoundariesAttributeIndependently(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	nonce := time.Now().UnixNano()
	labelA := fmt.Sprintf("concurrent-a-%d", nonce)
	labelB := fmt.Sprintf("concurrent-b-%d", nonce)

	// Lock-step channels force the two boundaries' writes to interleave:
	// A writes, then B writes, then A, then B.
	aTurn := make(chan struct{}, 1)
	bTurn := make(chan struct{}, 1)
	aTurn <- struct{}{}
	errs := make(chan error, 2)

	run := func(label, table string, myTurn, otherTurn chan struct{}) {
		errs <- h.detector.InBoundary(ctx, label, func(ctx context.Context) error {
			conn, err := h.db.Conn(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = conn.Close() }()
			for i := 0; i < 2; i++ {
				<-myTurn
				_, err := conn.ExecContext(ctx, "INSERT INTO "+table+" (v) VALUES (15)")
				otherTurn <- struct{}{}
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
	go run(labelA, "e2e_users", aTurn, bTurn)
	go run(labelB, "e2e_audit", bTurn, aTurn)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent boundary: %v", err)
		}
	}

	for _, label := range []string{labelA, labelB} {
		v := h.clientViolation(label)
		if v == nil {
			t.Fatalf("txnproof: expected a violation for boundary %q", label)
		}
		if v.WriteUnits != 2 || writeCount(v) != 2 {
			t.Errorf("txnproof: boundary %q: expected exactly its own 2 units / 2 writes, got units=%d writes=%d", label, v.WriteUnits, writeCount(v))
		}
	}
}
