package pgcheck

import (
	"errors"
	"regexp"
	"strings"
	"testing"

	txnproof "github.com/moznion/go-txnproof"
	"github.com/moznion/go-txnproof/crosscheck"
)

func newChecker(t *testing.T, opts ...Option) *Checker {
	t.Helper()
	c, err := New(opts...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func mustParse(t *testing.T, c *Checker, log string) []crosscheck.Statement {
	t.Helper()
	stmts, err := c.Parse(strings.NewReader(log))
	if err != nil {
		t.Fatal(err)
	}
	return stmts
}

// Fixtures use the default prefix (log_line_prefix = '%m [%p] %q%x %v ')
// unless stated otherwise. Both log_min_duration_statement-style lines
// ("duration: ... statement: ...") and log_statement-style lines
// ("statement: ...") appear; the parser must handle both.

const singleTxLog = `2026-07-18 10:00:00.000 UTC [77] 0 3/10 LOG:  duration: 0.031 ms  statement: BEGIN
2026-07-18 10:00:00.001 UTC [77] 0 3/10 LOG:  duration: 0.120 ms  statement: SELECT * FROM users WHERE id = 1
2026-07-18 10:00:00.002 UTC [77] 1001 3/10 LOG:  duration: 0.310 ms  statement: INSERT INTO users (id) VALUES (1)
2026-07-18 10:00:00.003 UTC [77] 1001 3/10 LOG:  duration: 0.200 ms  statement: UPDATE counters SET n = n + 1
2026-07-18 10:00:00.004 UTC [77] 0 3/10 LOG:  duration: 0.150 ms  statement: COMMIT
`

func TestSingleTransactionIsAtomic(t *testing.T) {
	c := newChecker(t)
	rep, err := c.Verify(strings.NewReader(singleTxLog))
	if err != nil {
		t.Fatalf("expected atomic scenario, got error: %v", err)
	}
	if !rep.Atomic() || rep.WriteUnits != 1 {
		t.Fatalf("expected 1 write unit, got %+v", rep)
	}
	u := rep.Units[0]
	if u.TxID != "3/10" {
		t.Errorf("unexpected unit identity (want the virtual txid): %+v", u)
	}
	if len(u.Statements) != 2 {
		t.Errorf("expected 2 write statements in the unit, got %+v", u.Statements)
	}
}

const twoAutoCommitLog = `2026-07-18 10:00:01.000 UTC [77] 1001 3/11 LOG:  duration: 0.300 ms  statement: INSERT INTO users (id) VALUES (1)
2026-07-18 10:00:01.001 UTC [77] 1002 3/12 LOG:  duration: 0.200 ms  statement: UPDATE counters SET n = n + 1
`

func TestTwoAutoCommitWritesIsNotAtomic(t *testing.T) {
	c := newChecker(t)
	rep, err := c.Verify(strings.NewReader(twoAutoCommitLog))
	var nae *crosscheck.NonAtomicError
	if !errors.As(err, &nae) {
		t.Fatalf("expected *crosscheck.NonAtomicError, got %v", err)
	}
	if rep == nil || rep.WriteUnits != 2 || nae.Report != rep {
		t.Fatalf("expected report with 2 units returned alongside the error, got %+v", rep)
	}
	msg := err.Error()
	for _, want := range []string{"span 2 server-side transactions", "3/11", "3/12", "INSERT INTO users", "UPDATE counters"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message should contain %q:\n%s", want, msg)
		}
	}
}

const rolledBackTxPlusWriteLog = `2026-07-18 10:00:02.000 UTC [77] 0 3/20 LOG:  duration: 0.030 ms  statement: BEGIN
2026-07-18 10:00:02.001 UTC [77] 900 3/20 LOG:  duration: 0.250 ms  statement: INSERT INTO users (id) VALUES (1)
2026-07-18 10:00:02.002 UTC [77] 900 3/20 LOG:  duration: 0.040 ms  statement: ROLLBACK
2026-07-18 10:00:02.003 UTC [77] 901 3/21 LOG:  duration: 0.210 ms  statement: INSERT INTO audit (id) VALUES (1)
`

func TestRolledBackTxStillCountsAsUnit(t *testing.T) {
	c := newChecker(t)
	rep, err := c.Verify(strings.NewReader(rolledBackTxPlusWriteLog))
	var nae *crosscheck.NonAtomicError
	if !errors.As(err, &nae) {
		t.Fatalf("expected *crosscheck.NonAtomicError, got %v", err)
	}
	if rep.WriteUnits != 2 {
		t.Fatalf("expected 2 write units (rolled-back tx still counts), got %+v", rep)
	}
}

var continuationLog = "2026-07-18 10:00:03.000 UTC [77] 500 3/30 LOG:  duration: 0.400 ms  statement: INSERT INTO t (a, b)\n" +
	"\tVALUES (1,\n" +
	"\t        2)\n" +
	"2026-07-18 10:00:03.001 UTC [77] 0 3/31 LOG:  duration: 0.100 ms  statement: SELECT * FROM t\n"

func TestContinuationLinesAreReassembled(t *testing.T) {
	c := newChecker(t)
	stmts := mustParse(t, c, continuationLog)
	if len(stmts) != 2 {
		t.Fatalf("expected 2 statements, got %+v", stmts)
	}
	want := "INSERT INTO t (a, b)\nVALUES (1,\n        2)"
	if stmts[0].SQL != want {
		t.Errorf("continuation lines not reassembled:\ngot  %q\nwant %q", stmts[0].SQL, want)
	}
	if stmts[0].Kind != txnproof.KindWrite || stmts[0].Line != 1 {
		t.Errorf("unexpected first statement: %+v", stmts[0])
	}
	if stmts[1].SQL != "SELECT * FROM t" || stmts[1].Kind != txnproof.KindRead {
		t.Errorf("unexpected second statement: %+v", stmts[1])
	}

	rep, err := c.Verify(strings.NewReader(continuationLog))
	if err != nil || rep.WriteUnits != 1 {
		t.Fatalf("expected 1 write unit, got %+v (err %v)", rep, err)
	}
}

const readsOnlyLog = `2026-07-18 10:00:04.000 UTC [77] 0 3/40 LOG:  duration: 0.100 ms  statement: SELECT * FROM users
2026-07-18 10:00:04.001 UTC [77] 0 3/41 LOG:  duration: 0.090 ms  statement: SELECT * FROM orders
`

func TestReadsWithZeroTxidAreIgnored(t *testing.T) {
	c := newChecker(t)
	rep, err := c.Verify(strings.NewReader(readsOnlyLog))
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

// log_statement = 'all' style: lines are logged before execution, so %x is
// still 0 even for writes; grouping must fall back to %v.
const logStatementStyleLog = `2026-07-18 10:00:05.000 UTC [77] 0 3/50 LOG:  statement: INSERT INTO a (id) VALUES (1)
2026-07-18 10:00:05.001 UTC [77] 0 3/51 LOG:  statement: INSERT INTO b (id) VALUES (1)
`

func TestVirtualTxidGroupsWritesWhenXidIsZero(t *testing.T) {
	c := newChecker(t)
	rep, err := c.Verify(strings.NewReader(logStatementStyleLog))
	var nae *crosscheck.NonAtomicError
	if !errors.As(err, &nae) {
		t.Fatalf("expected *crosscheck.NonAtomicError, got %v", err)
	}
	if rep.WriteUnits != 2 {
		t.Fatalf("expected 2 write units grouped by vxid, got %+v", rep)
	}
	if rep.Units[0].TxID != "3/50" {
		t.Errorf("unexpected first unit: %+v", rep.Units[0])
	}
}

// Extended query protocol under log_min_duration_statement: parse and bind
// phases are logged separately and must not be counted as statements; DETAIL
// lines must be skipped too.
const extendedProtocolLog = `2026-07-18 10:00:06.000 UTC [77] 0 3/60 LOG:  duration: 0.050 ms  parse <unnamed>: INSERT INTO a (id) VALUES ($1)
2026-07-18 10:00:06.001 UTC [77] 0 3/60 LOG:  duration: 0.020 ms  bind <unnamed>: INSERT INTO a (id) VALUES ($1)
2026-07-18 10:00:06.002 UTC [77] 1100 3/60 LOG:  duration: 0.300 ms  execute <unnamed>: INSERT INTO a (id) VALUES ($1)
2026-07-18 10:00:06.002 UTC [77] 1100 3/60 DETAIL:  Parameters: $1 = '1'
`

func TestExtendedProtocolExecuteLinesOnly(t *testing.T) {
	c := newChecker(t)
	stmts := mustParse(t, c, extendedProtocolLog)
	if len(stmts) != 1 {
		t.Fatalf("expected only the execute line to count, got %+v", stmts)
	}
	if stmts[0].TxID != "3/60" || stmts[0].Kind != txnproof.KindWrite {
		t.Errorf("unexpected statement: %+v", stmts[0])
	}
	rep, err := c.Verify(strings.NewReader(extendedProtocolLog))
	if err != nil || rep.WriteUnits != 1 {
		t.Fatalf("expected 1 write unit, got %+v (err %v)", rep, err)
	}
}

const scenarioLog = `2026-07-18 10:00:07.000 UTC [76] 800 3/70 LOG:  duration: 0.200 ms  statement: INSERT INTO noise_before (id) VALUES (1)
2026-07-18 10:00:07.001 UTC [77] 0 4/10 LOG:  duration: 0.050 ms  statement: SELECT 'txnproof:begin:create-user'
2026-07-18 10:00:07.002 UTC [77] 0 4/11 LOG:  duration: 0.030 ms  statement: BEGIN
2026-07-18 10:00:07.003 UTC [77] 1201 4/11 LOG:  duration: 0.300 ms  statement: INSERT INTO users (id) VALUES (1)
2026-07-18 10:00:07.004 UTC [77] 1201 4/11 LOG:  duration: 0.200 ms  statement: UPDATE counters SET n = n + 1
2026-07-18 10:00:07.005 UTC [77] 0 4/11 LOG:  duration: 0.100 ms  statement: COMMIT
2026-07-18 10:00:07.006 UTC [77] 0 4/12 LOG:  duration: 0.050 ms  statement: SELECT 'txnproof:end:create-user'
2026-07-18 10:00:07.007 UTC [76] 801 3/71 LOG:  duration: 0.200 ms  statement: INSERT INTO noise_after (id) VALUES (1)
`

func TestVerifyScenarioSlicesByMarkers(t *testing.T) {
	c := newChecker(t)
	rep, err := c.VerifyScenario(strings.NewReader(scenarioLog), "create-user")
	if err != nil {
		t.Fatalf("expected atomic scenario, got error: %v", err)
	}
	if rep.WriteUnits != 1 || rep.Units[0].TxID != "4/11" {
		t.Fatalf("noise outside the markers must be excluded, got %+v", rep)
	}

	// Without slicing, the noise writes make the same log non-atomic.
	if _, err := c.Verify(strings.NewReader(scenarioLog)); err == nil {
		t.Fatal("whole log should not be atomic")
	}
}

func TestVerifyScenarioMissingMarkers(t *testing.T) {
	c := newChecker(t)
	if _, err := c.VerifyScenario(strings.NewReader(singleTxLog), "create-user"); err == nil ||
		!strings.Contains(err.Error(), "begin marker") {
		t.Fatalf("expected begin-marker error, got %v", err)
	}
	if _, err := c.VerifyScenario(strings.NewReader(scenarioLog), "unknown"); err == nil {
		t.Fatal("expected error for unknown scenario label")
	}

	truncated := strings.SplitAfter(scenarioLog, "BEGIN\n")[0] // begin marker present, end marker missing
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

func TestUnattributableWriteIsAnError(t *testing.T) {
	// Prefix without %v, log_statement-style line: the write's %x is still
	// 0, so the check cannot attribute it to any transaction.
	c := newChecker(t, WithLogLinePrefix("%m [%p] %q%x "))
	log := "2026-07-18 10:00:08.000 UTC [77] 0 LOG:  statement: INSERT INTO a (id) VALUES (1)\n"
	_, err := c.Verify(strings.NewReader(log))
	if err == nil || !strings.Contains(err.Error(), "log_line_prefix") {
		t.Fatalf("expected an actionable attribution error, got %v", err)
	}
	var me *crosscheck.MissingTxIDError
	if !errors.As(err, &me) {
		t.Fatalf("the PostgreSQL advice must wrap the crosscheck error, got %v", err)
	}
	var nae *crosscheck.NonAtomicError
	if errors.As(err, &nae) {
		t.Fatal("attribution failure must not be reported as NonAtomicError")
	}
}

// On recent PostgreSQL versions the duration line of an auto-commit
// statement is emitted after its implicit transaction already ended: %x is
// back to 0 and %v renders as "0/0" ("N/0" on older versions). Neither is
// an identity — grouping by it would merge unrelated writes into one fake
// unit and silently pass a non-atomic scenario.
const clearedIdentifiersLog = `2026-07-18 10:00:11.000 UTC [77] 0 0/0 LOG:  duration: 0.300 ms  statement: INSERT INTO a (id) VALUES (1)
2026-07-18 10:00:11.001 UTC [77] 0 3/0 LOG:  duration: 0.200 ms  statement: INSERT INTO b (id) VALUES (1)
`

func TestClearedIdentifiersAreNotAnIdentity(t *testing.T) {
	c := newChecker(t)
	_, err := c.Verify(strings.NewReader(clearedIdentifiersLog))
	var me *crosscheck.MissingTxIDError
	if !errors.As(err, &me) {
		t.Fatalf("expected *crosscheck.MissingTxIDError, got %v", err)
	}
	var nae *crosscheck.NonAtomicError
	if errors.As(err, &nae) {
		t.Fatal("cleared identifiers must not silently group writes")
	}
}

func TestXidFallbackWhenPrefixLacksVxid(t *testing.T) {
	// Same %v-less prefix, but the lines are logged at completion so %x is
	// assigned: grouping must fall back to the real transaction ID.
	c := newChecker(t, WithLogLinePrefix("%m [%p] %q%x "))
	log := `2026-07-18 10:00:08.100 UTC [77] 1400 LOG:  duration: 0.300 ms  statement: INSERT INTO a (id) VALUES (1)
2026-07-18 10:00:08.101 UTC [77] 1400 LOG:  duration: 0.200 ms  statement: UPDATE a SET n = 1
2026-07-18 10:00:08.102 UTC [77] 1401 LOG:  duration: 0.200 ms  statement: INSERT INTO b (id) VALUES (1)
`
	rep, err := c.Verify(strings.NewReader(log))
	var nae *crosscheck.NonAtomicError
	if !errors.As(err, &nae) {
		t.Fatalf("expected *crosscheck.NonAtomicError, got %v", err)
	}
	if rep.WriteUnits != 2 || rep.Units[0].TxID != "xid 1400" || rep.Units[1].TxID != "xid 1401" {
		t.Fatalf("expected 2 units grouped by real txid, got %+v", rep)
	}
	if len(rep.Units[0].Statements) != 2 {
		t.Errorf("writes sharing a txid must share a unit, got %+v", rep.Units[0])
	}
}

func TestCustomLogLinePrefixTranslation(t *testing.T) {
	c := newChecker(t, WithLogLinePrefix("%t [%p]: [%l-1] user=%u,db=%d,xid=%x,vxid=%v "))
	log := "2026-07-18 10:00:09 UTC [77]: [5-1] user=app,db=test,xid=42,vxid=5/9 LOG:  statement: INSERT INTO a (id) VALUES (1)\n"
	stmts := mustParse(t, c, log)
	if len(stmts) != 1 || stmts[0].TxID != "5/9" {
		t.Fatalf("expected the virtual txid to win as the identity: %+v", stmts)
	}
}

func TestCustomPrefixPattern(t *testing.T) {
	re := regexp.MustCompile(`^\S+ (?P<vxid>\d+/\d+) `)
	c := newChecker(t, WithPrefixPattern(re))
	log := "backend1 7/33 LOG:  statement: DELETE FROM a\n"
	stmts := mustParse(t, c, log)
	if len(stmts) != 1 || stmts[0].TxID != "7/33" {
		t.Fatalf("unexpected parse result: %+v", stmts)
	}
}

func TestNewRejectsBadConfiguration(t *testing.T) {
	if _, err := New(WithLogLinePrefix("%Z ")); err == nil ||
		!strings.Contains(err.Error(), "unsupported log_line_prefix escape") {
		t.Errorf("expected unsupported-escape error, got %v", err)
	}
	if _, err := New(WithLogLinePrefix("%x %")); err == nil ||
		!strings.Contains(err.Error(), "bare %") {
		t.Errorf("expected bare-percent error, got %v", err)
	}
	if _, err := New(WithLogLinePrefix("%m [%p] ")); err == nil ||
		!strings.Contains(err.Error(), "neither") {
		t.Errorf("expected missing-group error for prefix without %%x/%%v, got %v", err)
	}
	if _, err := New(WithPrefixPattern(regexp.MustCompile(`^\S+ `))); err == nil {
		t.Error("expected missing-group error for pattern without named groups")
	}
}

func TestWithClassifierOverrides(t *testing.T) {
	// Treat everything as a write: the two SELECTs run in distinct virtual
	// transactions and must now violate.
	c := newChecker(t, WithClassifier(func(string) txnproof.StatementKind { return txnproof.KindWrite }))
	_, err := c.Verify(strings.NewReader(readsOnlyLog))
	var nae *crosscheck.NonAtomicError
	if !errors.As(err, &nae) {
		t.Fatalf("expected *crosscheck.NonAtomicError with the custom classifier, got %v", err)
	}
}

func TestNonStatementLinesAreSkipped(t *testing.T) {
	c := newChecker(t)
	log := `2026-07-18 10:00:10.000 UTC [1] LOG:  checkpoint starting: time
2026-07-18 10:00:10.001 UTC [77] 0 3/80 ERROR:  duplicate key value violates unique constraint "users_pkey"
2026-07-18 10:00:10.002 UTC [77] 0 3/80 STATEMENT:  INSERT INTO users (id) VALUES (1)
2026-07-18 10:00:10.003 UTC [77] 0 3/81 LOG:  duration: 0.100 ms
2026-07-18 10:00:10.004 UTC [77] 1300 3/82 LOG:  duration: 0.300 ms  statement: INSERT INTO a (id) VALUES (1)
`
	stmts := mustParse(t, c, log)
	if len(stmts) != 1 || stmts[0].TxID != "3/82" {
		t.Fatalf("expected only the statement-carrying LOG line to be parsed, got %+v", stmts)
	}
}

func TestReportStringWhenAtomic(t *testing.T) {
	c := newChecker(t)
	rep, err := c.Verify(strings.NewReader(singleTxLog))
	if err != nil {
		t.Fatal(err)
	}
	s := rep.String()
	if !strings.Contains(s, "one server-side transaction") || !strings.Contains(s, "3/10") {
		t.Errorf("unexpected atomic report string: %s", s)
	}
}
