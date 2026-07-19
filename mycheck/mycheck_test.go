package mycheck

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	txnproof "github.com/moznion/go-txnproof"
	"github.com/moznion/go-txnproof/crosscheck"
)

func mustParse(t *testing.T, c *Checker, log string) []crosscheck.Statement {
	t.Helper()
	stmts, err := c.Parse(strings.NewReader(log))
	if err != nil {
		t.Fatal(err)
	}
	return stmts
}

// l renders one general-log entry line the way MySQL writes it: ISO-8601 UTC
// timestamp, tab, thread id right-aligned to width 5, space, command, tab,
// argument (format validated against real MySQL 9.7 output; see
// realPreparedScenarioLog for a verbatim capture).
func l(id int, cmd, arg string) string {
	return fmt.Sprintf("2026-07-18T01:58:59.000000Z\t%5d %s\t%s\n", id, cmd, arg)
}

const banner = `/opt/homebrew/opt/mysql/bin/mysqld, Version: 9.7.1 (Homebrew). started with:
Tcp port: 0  Unix socket: /tmp/txnproof-mycheck.sock
Time                 Id Command    Argument
`

func TestBannerAndNonStatementEntriesAreSkipped(t *testing.T) {
	log := banner +
		l(11, "Connect", "root@localhost on txp using Socket") +
		l(11, "Query", "INSERT INTO users VALUES (1, 1)") +
		l(11, "Quit", "")
	c := New()
	stmts := mustParse(t, c, log)
	if len(stmts) != 1 {
		t.Fatalf("expected only the Query entry to yield a statement, got %+v", stmts)
	}
	if stmts[0].SQL != "INSERT INTO users VALUES (1, 1)" || stmts[0].Kind != txnproof.KindWrite || stmts[0].Line != 5 {
		t.Errorf("unexpected statement: %+v", stmts[0])
	}
}

func TestSingleTransactionIsAtomic(t *testing.T) {
	log := banner +
		l(11, "Query", "SELECT * FROM users WHERE id = 1") +
		l(11, "Query", "BEGIN") +
		l(11, "Query", "INSERT INTO users VALUES (1, 1)") +
		l(11, "Query", "UPDATE users SET v = 2 WHERE id = 1") +
		l(11, "Query", "COMMIT")
	c := New()
	rep, err := c.Verify(strings.NewReader(log))
	if err != nil {
		t.Fatalf("expected atomic scenario, got error: %v", err)
	}
	if !rep.Atomic() || rep.WriteUnits != 1 {
		t.Fatalf("expected 1 write unit, got %+v", rep)
	}
	u := rep.Units[0]
	if u.TxID != "thread 11 tx 1" {
		t.Errorf("unexpected unit identity: %+v", u)
	}
	if len(u.Statements) != 2 {
		t.Errorf("expected 2 write statements in the unit, got %+v", u.Statements)
	}
}

func TestTwoAutoCommitWritesIsNotAtomic(t *testing.T) {
	log := banner +
		l(12, "Query", "INSERT INTO users VALUES (2, 1)") +
		l(12, "Query", "INSERT INTO audit VALUES (2, 1)")
	c := New()
	rep, err := c.Verify(strings.NewReader(log))
	var nae *crosscheck.NonAtomicError
	if !errors.As(err, &nae) {
		t.Fatalf("expected *crosscheck.NonAtomicError, got %v", err)
	}
	if rep == nil || rep.WriteUnits != 2 || nae.Report != rep {
		t.Fatalf("expected report with 2 units returned alongside the error, got %+v", rep)
	}
	msg := err.Error()
	for _, want := range []string{"span 2 server-side transactions", "thread 12 stmt 1", "thread 12 stmt 2", "INSERT INTO users", "INSERT INTO audit"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message should contain %q:\n%s", want, msg)
		}
	}
}

func TestRolledBackTxStillCountsAsUnit(t *testing.T) {
	log := banner +
		l(13, "Query", "START TRANSACTION") +
		l(13, "Query", "INSERT INTO users VALUES (3, 1)") +
		l(13, "Query", "ROLLBACK") +
		l(13, "Query", "INSERT INTO audit VALUES (3, 1)")
	c := New()
	rep, err := c.Verify(strings.NewReader(log))
	var nae *crosscheck.NonAtomicError
	if !errors.As(err, &nae) {
		t.Fatalf("expected *crosscheck.NonAtomicError, got %v", err)
	}
	if rep.WriteUnits != 2 {
		t.Fatalf("expected 2 write units (rolled-back tx still counts), got %+v", rep)
	}
	if rep.Units[0].TxID != "thread 13 tx 1" || rep.Units[1].TxID != "thread 13 stmt 4" {
		t.Errorf("unexpected unit identities: %+v", rep.Units)
	}
}

func TestSavepointRollbackKeepsTransactionOpen(t *testing.T) {
	log := banner +
		l(14, "Query", "BEGIN") +
		l(14, "Query", "INSERT INTO users VALUES (4, 1)") +
		l(14, "Query", "SAVEPOINT sp") +
		l(14, "Query", "INSERT INTO audit VALUES (4, 1)") +
		l(14, "Query", "ROLLBACK TO SAVEPOINT sp") +
		l(14, "Query", "INSERT INTO audit VALUES (5, 1)") +
		l(14, "Query", "COMMIT")
	c := New()
	rep, err := c.Verify(strings.NewReader(log))
	if err != nil {
		t.Fatalf("expected atomic scenario (ROLLBACK TO SAVEPOINT must not end the tx), got %v", err)
	}
	if rep.WriteUnits != 1 || len(rep.Units[0].Statements) != 3 {
		t.Fatalf("expected 1 unit with all 3 writes, got %+v", rep)
	}
}

func TestContinuationLinesAreReassembled(t *testing.T) {
	log := banner +
		l(18, "Query", "INSERT INTO users (id, v)") +
		"VALUES (10,\n" +
		"        1)\n" +
		l(18, "Query", "SELECT * FROM users")
	c := New()
	stmts := mustParse(t, c, log)
	if len(stmts) != 2 {
		t.Fatalf("expected 2 statements, got %+v", stmts)
	}
	want := "INSERT INTO users (id, v)\nVALUES (10,\n        1)"
	if stmts[0].SQL != want {
		t.Errorf("continuation lines not reassembled:\ngot  %q\nwant %q", stmts[0].SQL, want)
	}
	if stmts[0].Kind != txnproof.KindWrite || stmts[0].Line != 4 {
		t.Errorf("unexpected first statement: %+v", stmts[0])
	}
	if stmts[1].SQL != "SELECT * FROM users" || stmts[1].Kind != txnproof.KindRead {
		t.Errorf("unexpected second statement: %+v", stmts[1])
	}

	rep, err := c.Verify(strings.NewReader(log))
	if err != nil || rep.WriteUnits != 1 {
		t.Fatalf("expected 1 write unit, got %+v (err %v)", rep, err)
	}
}

func TestReadsNeverCount(t *testing.T) {
	log := banner +
		l(19, "Query", "SELECT * FROM users") +
		l(19, "Query", "select @@version_comment limit 1") +
		l(19, "Query", "SHOW TABLES")
	c := New()
	rep, err := c.Verify(strings.NewReader(log))
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Atomic() || rep.WriteUnits != 0 {
		t.Fatalf("reads must not count as write units, got %+v", rep)
	}
	if !strings.Contains(rep.String(), "no writes") {
		t.Errorf("unexpected report string: %s", rep.String())
	}
}

// The server-side prepared-statement path: the SQL-level "PREPARE ... FROM"
// and "EXECUTE stmt" Query lines classify as neither read nor write, the
// "Prepare" entry is ignored, and only the "Execute" entries — logged with
// the parameters substituted — count. Two auto-commit executions must be
// exactly 2 single-statement units, no double-counting.
func TestPreparedExecutePathCountsExecutionsOnce(t *testing.T) {
	log := banner +
		l(17, "Query", "PREPARE ins FROM ...") +
		l(17, "Prepare", "INSERT INTO users VALUES (?, ?)") +
		l(17, "Query", "EXECUTE ins USING @a, @b") +
		l(17, "Execute", "INSERT INTO users VALUES (8, 1)") +
		l(17, "Query", "EXECUTE ins USING @a, @b") +
		l(17, "Execute", "INSERT INTO users VALUES (9, 1)") +
		l(17, "Query", "DEALLOCATE PREPARE ins")
	c := New()
	rep, err := c.Verify(strings.NewReader(log))
	var nae *crosscheck.NonAtomicError
	if !errors.As(err, &nae) {
		t.Fatalf("expected *crosscheck.NonAtomicError, got %v", err)
	}
	if rep.WriteUnits != 2 {
		t.Fatalf("expected exactly 2 units for the 2 executions, got %+v", rep)
	}
	for _, u := range rep.Units {
		if len(u.Statements) != 1 {
			t.Errorf("expected 1 statement per unit (no double-counting), got %+v", u)
		}
	}
}

// Two threads interleaved in one log must group independently: thread 21
// runs one transaction with two writes (1 unit), thread 22 runs two
// auto-commit writes (2 units) in between.
func TestInterleavedThreadsGroupIndependently(t *testing.T) {
	log := banner +
		l(21, "Query", "BEGIN") +
		l(22, "Query", "INSERT INTO other VALUES (1)") +
		l(21, "Query", "INSERT INTO users VALUES (1, 1)") +
		l(22, "Query", "INSERT INTO other VALUES (2)") +
		l(21, "Query", "INSERT INTO audit VALUES (1, 1)") +
		l(21, "Query", "COMMIT")
	c := New()
	rep, err := c.Verify(strings.NewReader(log))
	var nae *crosscheck.NonAtomicError
	if !errors.As(err, &nae) {
		t.Fatalf("expected *crosscheck.NonAtomicError, got %v", err)
	}
	if rep.WriteUnits != 3 {
		t.Fatalf("expected 3 units (1 tx + 2 auto-commit), got %+v", rep)
	}
	byID := map[string]int{}
	for _, u := range rep.Units {
		byID[u.TxID] = len(u.Statements)
	}
	if byID["thread 21 tx 1"] != 2 {
		t.Errorf("thread 21's transaction must contain both of its writes, got %+v", rep.Units)
	}
	if byID["thread 22 stmt 1"] != 1 || byID["thread 22 stmt 2"] != 1 {
		t.Errorf("thread 22's auto-commit writes must be their own units, got %+v", rep.Units)
	}
}

// BEGIN while a transaction is already open implicitly commits the open one:
// the two INSERTs land in two different transactions.
func TestBeginWhileOpenImplicitlyCommits(t *testing.T) {
	log := banner +
		l(16, "Query", "BEGIN") +
		l(16, "Query", "INSERT INTO users VALUES (7, 1)") +
		l(16, "Query", "BEGIN") +
		l(16, "Query", "INSERT INTO audit VALUES (7, 1)") +
		l(16, "Query", "COMMIT")
	c := New()
	rep, err := c.Verify(strings.NewReader(log))
	var nae *crosscheck.NonAtomicError
	if !errors.As(err, &nae) {
		t.Fatalf("expected *crosscheck.NonAtomicError, got %v", err)
	}
	if rep.WriteUnits != 2 || rep.Units[0].TxID != "thread 16 tx 1" || rep.Units[1].TxID != "thread 16 tx 2" {
		t.Fatalf("expected the writes split across 2 transactions, got %+v", rep)
	}
}

// DDL inside a transaction implicitly commits it and is its own auto-commit
// unit; the write after it runs auto-committed. 3 units total.
func TestDDLInsideTxImplicitlyCommits(t *testing.T) {
	log := banner +
		l(15, "Query", "BEGIN") +
		l(15, "Query", "INSERT INTO users VALUES (6, 1)") +
		l(15, "Query", "CREATE TABLE t2 (id INT)") +
		l(15, "Query", "INSERT INTO audit VALUES (6, 1)") +
		l(15, "Query", "COMMIT")
	c := New()
	rep, err := c.Verify(strings.NewReader(log))
	var nae *crosscheck.NonAtomicError
	if !errors.As(err, &nae) {
		t.Fatalf("expected *crosscheck.NonAtomicError, got %v", err)
	}
	if rep.WriteUnits != 3 {
		t.Fatalf("expected 3 units (tx, DDL, auto-commit write), got %+v", rep)
	}
	if rep.Units[0].TxID != "thread 15 tx 1" || rep.Units[1].TxID != "thread 15 stmt 3" || rep.Units[2].TxID != "thread 15 stmt 4" {
		t.Errorf("unexpected unit identities: %+v", rep.Units)
	}
}

// CREATE TEMPORARY TABLE does not implicitly commit: it stays inside the
// transaction and the scenario remains one unit.
func TestTemporaryTableDDLDoesNotImplicitlyCommit(t *testing.T) {
	log := banner +
		l(15, "Query", "BEGIN") +
		l(15, "Query", "INSERT INTO users VALUES (6, 1)") +
		l(15, "Query", "CREATE TEMPORARY TABLE tmp (id INT)") +
		l(15, "Query", "INSERT INTO audit VALUES (6, 1)") +
		l(15, "Query", "COMMIT")
	c := New()
	rep, err := c.Verify(strings.NewReader(log))
	if err != nil {
		t.Fatalf("expected atomic scenario, got %v", err)
	}
	if rep.WriteUnits != 1 || len(rep.Units[0].Statements) != 3 {
		t.Fatalf("expected 1 unit containing all 3 writes, got %+v", rep)
	}
}

// A SET statement that touches autocommit inside the verified scenario is a
// hard error: with autocommit disabled the reconstruction rules are wrong,
// and guessing would produce a false verdict.
func TestAutocommitChangeIsAnActionableError(t *testing.T) {
	for _, stmt := range []string{
		"SET autocommit = 0",
		"SET autocommit=1",
		"SET @@session.autocommit := 0",
		"SET SESSION autocommit = OFF",
	} {
		log := banner +
			l(23, "Query", stmt) +
			l(23, "Query", "INSERT INTO users VALUES (1, 1)")
		c := New()
		_, err := c.Verify(strings.NewReader(log))
		if err == nil || !strings.Contains(err.Error(), "autocommit") {
			t.Fatalf("%s: expected an actionable autocommit error, got %v", stmt, err)
		}
		var nae *crosscheck.NonAtomicError
		if errors.As(err, &nae) {
			t.Errorf("%s: the autocommit failure must not be reported as a verdict", stmt)
		}
	}
}

// SET statements not touching autocommit (user variables, other session
// variables) must not trip the autocommit rejection.
func TestHarmlessSetStatementsAreAccepted(t *testing.T) {
	log := banner +
		l(23, "Query", "SET @a = 8") +
		l(23, "Query", "SET SESSION sql_mode = 'TRADITIONAL'") +
		l(23, "Query", "INSERT INTO users VALUES (1, 1)")
	c := New()
	rep, err := c.Verify(strings.NewReader(log))
	if err != nil || rep.WriteUnits != 1 {
		t.Fatalf("expected 1 write unit, got %+v (err %v)", rep, err)
	}
}

// An autocommit SET outside the marker-delimited scenario is ignored; only
// the verified slice is checked.
func TestAutocommitChangeOutsideScenarioIsIgnored(t *testing.T) {
	log := banner +
		l(24, "Query", "SET autocommit = 1") +
		l(24, "Query", "SELECT 'txnproof:begin:my-scenario'") +
		l(24, "Query", "BEGIN") +
		l(24, "Query", "INSERT INTO users VALUES (1, 1)") +
		l(24, "Query", "COMMIT") +
		l(24, "Query", "SELECT 'txnproof:end:my-scenario'")
	c := New()
	rep, err := c.VerifyScenario(strings.NewReader(log), "my-scenario")
	if err != nil || rep.WriteUnits != 1 {
		t.Fatalf("expected the out-of-scenario SET to be ignored, got %+v (err %v)", rep, err)
	}
}

// Quit while a transaction is open is an implicit rollback: the transaction
// closes, and its writes still count as a unit.
func TestQuitMidTransactionIsImplicitRollback(t *testing.T) {
	log := banner +
		l(19, "Query", "BEGIN") +
		l(19, "Query", "INSERT INTO users VALUES (11, 1)") +
		l(19, "Quit", "") +
		l(20, "Query", "INSERT INTO audit VALUES (11, 1)")
	c := New()
	rep, err := c.Verify(strings.NewReader(log))
	var nae *crosscheck.NonAtomicError
	if !errors.As(err, &nae) {
		t.Fatalf("expected *crosscheck.NonAtomicError (quit-rolled-back tx still counts), got %v", err)
	}
	if rep.WriteUnits != 2 {
		t.Fatalf("expected 2 write units, got %+v", rep)
	}
}

func scenarioLog() string {
	return banner +
		l(30, "Query", "INSERT INTO noise_before VALUES (1)") +
		l(31, "Query", "SELECT 'txnproof:begin:create-user'") +
		l(31, "Query", "BEGIN") +
		l(31, "Query", "INSERT INTO users VALUES (1, 1)") +
		l(31, "Query", "UPDATE counters SET n = n + 1") +
		l(31, "Query", "COMMIT") +
		l(31, "Query", "SELECT 'txnproof:end:create-user'") +
		l(30, "Query", "INSERT INTO noise_after VALUES (1)")
}

func TestVerifyScenarioSlicesByMarkers(t *testing.T) {
	c := New()
	rep, err := c.VerifyScenario(strings.NewReader(scenarioLog()), "create-user")
	if err != nil {
		t.Fatalf("expected atomic scenario, got error: %v", err)
	}
	if rep.WriteUnits != 1 || rep.Units[0].TxID != "thread 31 tx 1" {
		t.Fatalf("noise outside the markers must be excluded, got %+v", rep)
	}

	// Without slicing, the noise writes make the same log non-atomic.
	if _, err := c.Verify(strings.NewReader(scenarioLog())); err == nil {
		t.Fatal("whole log should not be atomic")
	}
}

func TestVerifyScenarioMissingMarkers(t *testing.T) {
	c := New()
	noMarkers := banner + l(11, "Query", "INSERT INTO users VALUES (1, 1)")
	if _, err := c.VerifyScenario(strings.NewReader(noMarkers), "create-user"); err == nil ||
		!strings.Contains(err.Error(), "begin marker") {
		t.Fatalf("expected begin-marker error, got %v", err)
	}
	if _, err := c.VerifyScenario(strings.NewReader(scenarioLog()), "unknown"); err == nil {
		t.Fatal("expected error for unknown scenario label")
	}

	truncated := strings.SplitAfter(scenarioLog(), "COMMIT\n")[0] // begin marker present, end marker missing
	if _, err := c.VerifyScenario(strings.NewReader(truncated), "create-user"); err == nil ||
		!strings.Contains(err.Error(), "end marker") {
		t.Fatalf("expected end-marker error, got %v", err)
	}
}

func TestMarkersAreReexported(t *testing.T) {
	if got, want := BeginMarker("create-user"), crosscheck.BeginMarker("create-user"); got != want {
		t.Errorf("BeginMarker: got %q, want %q", got, want)
	}
	if got, want := EndMarker("create-user"), crosscheck.EndMarker("create-user"); got != want {
		t.Errorf("EndMarker: got %q, want %q", got, want)
	}
}

func TestWithClassifierOverrides(t *testing.T) {
	// Treat everything as a write: the two auto-commit SELECTs must now
	// violate.
	log := banner +
		l(19, "Query", "SELECT * FROM users") +
		l(19, "Query", "SELECT * FROM audit")
	c := New(WithClassifier(func(string) txnproof.StatementKind { return txnproof.KindWrite }))
	_, err := c.Verify(strings.NewReader(log))
	var nae *crosscheck.NonAtomicError
	if !errors.As(err, &nae) {
		t.Fatalf("expected *crosscheck.NonAtomicError with the custom classifier, got %v", err)
	}
}

// A write with an empty TxID cannot come out of Parse, but statements built
// by hand can carry one; the wrapped error must keep the crosscheck type and
// point back at Verify / VerifyScenario.
func TestMissingTxIDAdvice(t *testing.T) {
	c := New()
	_, err := c.verifyStatements([]crosscheck.Statement{
		{SQL: "INSERT INTO users VALUES (1, 1)", Kind: txnproof.KindWrite},
	})
	if err == nil || !strings.Contains(err.Error(), "Checker.Parse") {
		t.Fatalf("expected the MySQL advice, got %v", err)
	}
	var me *crosscheck.MissingTxIDError
	if !errors.As(err, &me) {
		t.Fatalf("the advice must wrap the crosscheck error, got %v", err)
	}
}

// realPreparedScenarioLog is a verbatim capture (paths shortened) of the
// general query log MySQL 9.7.1 (Homebrew, macOS) wrote for the SQL-level
// prepared-statement scenario: PREPARE, two EXECUTEs, DEALLOCATE, wrapped in
// crosscheck markers. Note the server's own rendering quirks this pins down:
// the "PREPARE ins FROM '...'" Query line elides the statement literal to
// "..."; each EXECUTE produces both a Query line (the textual EXECUTE) and
// an Execute line with the parameters substituted; the Quit entry has an
// empty argument after its tab.
const realPreparedScenarioLog = `/opt/homebrew/opt/mysql/bin/mysqld, Version: 9.7.1 (Homebrew). started with:
Tcp port: 0  Unix socket: /tmp/txnproof-mycheck.sock
Time                 Id Command    Argument
2026-07-18T01:58:59.322377Z	   17 Connect	root@localhost on txp using Socket
2026-07-18T01:58:59.322406Z	   17 Query	select @@version_comment limit 1
2026-07-18T01:58:59.322449Z	   17 Query	SELECT 'txnproof:begin:prepare-sql'
2026-07-18T01:58:59.322473Z	   17 Query	PREPARE ins FROM ...
2026-07-18T01:58:59.322552Z	   17 Prepare	INSERT INTO users VALUES (?, ?)
2026-07-18T01:58:59.322569Z	   17 Query	SET @a = 8
2026-07-18T01:58:59.322597Z	   17 Query	SET @b = 1
2026-07-18T01:58:59.322614Z	   17 Query	EXECUTE ins USING @a, @b
2026-07-18T01:58:59.322619Z	   17 Execute	INSERT INTO users VALUES (8, 1)
2026-07-18T01:58:59.322721Z	   17 Query	SET @a = 9
2026-07-18T01:58:59.322735Z	   17 Query	EXECUTE ins USING @a, @b
2026-07-18T01:58:59.322739Z	   17 Execute	INSERT INTO users VALUES (9, 1)
2026-07-18T01:58:59.322845Z	   17 Query	DEALLOCATE PREPARE ins
2026-07-18T01:58:59.322860Z	   17 Query	SELECT 'txnproof:end:prepare-sql'
2026-07-18T01:58:59.322878Z	   17 Quit	
`

func TestRealCapturedLogPreparedScenario(t *testing.T) {
	c := New()
	rep, err := c.VerifyScenario(strings.NewReader(realPreparedScenarioLog), "prepare-sql")
	var nae *crosscheck.NonAtomicError
	if !errors.As(err, &nae) {
		t.Fatalf("expected *crosscheck.NonAtomicError for two auto-commit executions, got %v", err)
	}
	if rep.WriteUnits != 2 {
		t.Fatalf("expected 2 write units, got %+v", rep)
	}
	// The units must be the substituted Execute texts, one execution each.
	for i, want := range []string{"INSERT INTO users VALUES (8, 1)", "INSERT INTO users VALUES (9, 1)"} {
		u := rep.Units[i]
		if len(u.Statements) != 1 || u.Statements[0].SQL != want {
			t.Errorf("unit %d: expected exactly %q, got %+v", i, want, u)
		}
	}
}

// realSavepointScenarioLog is a verbatim capture (paths shortened) of the
// savepoint scenario from the same MySQL 9.7.1 run: ROLLBACK TO SAVEPOINT
// must not end the transaction, so all three INSERTs share one unit.
const realSavepointScenarioLog = `2026-07-18T01:58:59.297109Z	   14 Connect	root@localhost on txp using Socket
2026-07-18T01:58:59.297138Z	   14 Query	select @@version_comment limit 1
2026-07-18T01:58:59.297181Z	   14 Query	SELECT 'txnproof:begin:savepoint'
2026-07-18T01:58:59.297201Z	   14 Query	BEGIN
2026-07-18T01:58:59.297216Z	   14 Query	INSERT INTO users VALUES (4, 1)
2026-07-18T01:58:59.297259Z	   14 Query	SAVEPOINT sp
2026-07-18T01:58:59.297281Z	   14 Query	INSERT INTO audit VALUES (4, 1)
2026-07-18T01:58:59.297323Z	   14 Query	ROLLBACK TO SAVEPOINT sp
2026-07-18T01:58:59.297346Z	   14 Query	INSERT INTO audit VALUES (5, 1)
2026-07-18T01:58:59.297369Z	   14 Query	COMMIT
2026-07-18T01:58:59.297446Z	   14 Query	SELECT 'txnproof:end:savepoint'
2026-07-18T01:58:59.297461Z	   14 Quit	
`

func TestRealCapturedLogSavepointScenario(t *testing.T) {
	c := New()
	rep, err := c.VerifyScenario(strings.NewReader(realSavepointScenarioLog), "savepoint")
	if err != nil {
		t.Fatalf("expected atomic scenario, got %v", err)
	}
	if rep.WriteUnits != 1 || rep.Units[0].TxID != "thread 14 tx 1" || len(rep.Units[0].Statements) != 3 {
		t.Fatalf("expected 1 unit with all 3 writes, got %+v", rep)
	}
}
