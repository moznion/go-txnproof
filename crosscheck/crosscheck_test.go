package crosscheck

import (
	"bufio"
	"errors"
	"io"
	"strings"
	"testing"

	txnproof "github.com/moznion/go-txnproof"
)

func write(txID, sql string, line int) Statement {
	return Statement{SQL: sql, Kind: txnproof.KindWrite, TxID: txID, Line: line}
}

func read(txID, sql string, line int) Statement {
	return Statement{SQL: sql, Kind: txnproof.KindRead, TxID: txID, Line: line}
}

func TestCheckGroupsWritesByTxID(t *testing.T) {
	stmts := []Statement{
		{SQL: "BEGIN", Kind: txnproof.KindBegin, TxID: "t1", Line: 1},
		read("t1", "SELECT * FROM users", 2),
		write("t1", "INSERT INTO users (id) VALUES (1)", 3),
		write("t1", "UPDATE counters SET n = n + 1", 4),
		{SQL: "COMMIT", Kind: txnproof.KindCommit, TxID: "t1", Line: 5},
	}
	rep, err := Check(stmts)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Atomic() || rep.WriteUnits != 1 || len(rep.Units) != 1 {
		t.Fatalf("expected 1 write unit, got %+v", rep)
	}
	u := rep.Units[0]
	if u.TxID != "t1" || len(u.Statements) != 2 {
		t.Fatalf("unexpected unit: %+v", u)
	}
}

func TestCheckSeparateTxIDsAreSeparateUnits(t *testing.T) {
	stmts := []Statement{
		write("t2", "INSERT INTO users (id) VALUES (1)", 1),
		write("t3", "UPDATE counters SET n = n + 1", 2),
		write("t2", "INSERT INTO users (id) VALUES (2)", 3),
	}
	rep, err := Check(stmts)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Atomic() || rep.WriteUnits != 2 {
		t.Fatalf("expected 2 write units, got %+v", rep)
	}
	// Units appear in order of first appearance, and later statements of an
	// already-seen transaction join its unit.
	if rep.Units[0].TxID != "t2" || len(rep.Units[0].Statements) != 2 {
		t.Errorf("unexpected first unit: %+v", rep.Units[0])
	}
	if rep.Units[1].TxID != "t3" || len(rep.Units[1].Statements) != 1 {
		t.Errorf("unexpected second unit: %+v", rep.Units[1])
	}
}

func TestCheckIgnoresNonWrites(t *testing.T) {
	stmts := []Statement{
		read("", "SELECT 1", 1), // reads may carry no identity
		read("t1", "SELECT * FROM users", 2),
		{SQL: "SET search_path TO app", Kind: txnproof.KindOther, TxID: "t1", Line: 3},
	}
	rep, err := Check(stmts)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Atomic() || rep.WriteUnits != 0 {
		t.Fatalf("non-writes must not count as units, got %+v", rep)
	}
	if !strings.Contains(rep.String(), "no writes") {
		t.Errorf("unexpected report string: %s", rep.String())
	}
}

func TestCheckMissingTxIDOnWriteIsAnError(t *testing.T) {
	stmts := []Statement{
		write("", "INSERT INTO users (id) VALUES (1)", 7),
	}
	_, err := Check(stmts)
	var me *MissingTxIDError
	if !errors.As(err, &me) {
		t.Fatalf("expected *MissingTxIDError, got %v", err)
	}
	if me.Statement.Line != 7 {
		t.Errorf("error should carry the offending statement, got %+v", me.Statement)
	}
	for _, want := range []string{"line 7", "INSERT INTO users", "transaction identity"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error message should contain %q: %s", want, err)
		}
	}
}

func TestVerifyStatementsReturnsNonAtomicError(t *testing.T) {
	stmts := []Statement{
		write("t1", "INSERT INTO users (id) VALUES (1)", 1),
		write("t2", "UPDATE counters\n\tSET n = n + 1", 2),
	}
	rep, err := VerifyStatements(stmts)
	var nae *NonAtomicError
	if !errors.As(err, &nae) {
		t.Fatalf("expected *NonAtomicError, got %v", err)
	}
	if rep == nil || rep.WriteUnits != 2 || nae.Report != rep {
		t.Fatalf("expected report with 2 units returned alongside the error, got %+v", rep)
	}
	msg := err.Error()
	for _, want := range []string{
		"span 2 server-side transactions",
		"transaction t1", "transaction t2",
		"INSERT INTO users", "UPDATE counters SET n = n + 1", // whitespace compacted
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message should contain %q:\n%s", want, msg)
		}
	}
}

func TestVerifyStatementsAtomic(t *testing.T) {
	rep, err := VerifyStatements([]Statement{
		write("t1", "INSERT INTO users (id) VALUES (1)", 1),
		write("t1", "UPDATE counters SET n = n + 1", 2),
	})
	if err != nil {
		t.Fatalf("expected atomic scenario, got %v", err)
	}
	s := rep.String()
	if !strings.Contains(s, "all 2 write(s)") || !strings.Contains(s, "t1") {
		t.Errorf("unexpected atomic report string: %s", s)
	}
}

func TestRolledBackTxStillCountsAsUnit(t *testing.T) {
	// The core does not need to see the ROLLBACK at all: the write's
	// transaction identity alone makes it a unit.
	stmts := []Statement{
		{SQL: "BEGIN", Kind: txnproof.KindBegin, TxID: "t1", Line: 1},
		write("t1", "INSERT INTO users (id) VALUES (1)", 2),
		{SQL: "ROLLBACK", Kind: txnproof.KindRollback, TxID: "t1", Line: 3},
		write("t2", "INSERT INTO audit (id) VALUES (1)", 4),
	}
	rep, err := VerifyStatements(stmts)
	var nae *NonAtomicError
	if !errors.As(err, &nae) {
		t.Fatalf("expected *NonAtomicError, got %v", err)
	}
	if rep.WriteUnits != 2 {
		t.Fatalf("expected 2 write units (rolled-back tx still counts), got %+v", rep)
	}
}

func TestLinelessStatementsRenderWithoutLineRef(t *testing.T) {
	_, err := VerifyStatements([]Statement{
		write("t1", "INSERT INTO a (id) VALUES (1)", 0),
		write("t2", "INSERT INTO b (id) VALUES (1)", 0),
	})
	if err == nil || strings.Contains(err.Error(), "line") {
		t.Fatalf("statements without line numbers must not render a line reference, got %v", err)
	}
}

func TestMarkers(t *testing.T) {
	if got, want := BeginMarker("create-user"), "SELECT 'txnproof:begin:create-user'"; got != want {
		t.Errorf("BeginMarker: got %q, want %q", got, want)
	}
	if got, want := EndMarker("create-user"), "SELECT 'txnproof:end:create-user'"; got != want {
		t.Errorf("EndMarker: got %q, want %q", got, want)
	}
	if got, want := BeginMarker("a'b"), "SELECT 'txnproof:begin:a''b'"; got != want {
		t.Errorf("BeginMarker must escape quotes: got %q, want %q", got, want)
	}
}

func TestScenarioSlicesByMarkers(t *testing.T) {
	stmts := []Statement{
		write("t0", "INSERT INTO noise_before (id) VALUES (1)", 1),
		read("t1", BeginMarker("create-user"), 2),
		write("t2", "INSERT INTO users (id) VALUES (1)", 3),
		write("t2", "UPDATE counters SET n = n + 1", 4),
		read("t3", EndMarker("create-user"), 5),
		write("t4", "INSERT INTO noise_after (id) VALUES (1)", 6),
	}
	sliced, err := Scenario(stmts, "create-user")
	if err != nil {
		t.Fatal(err)
	}
	if len(sliced) != 2 || sliced[0].Line != 3 || sliced[1].Line != 4 {
		t.Fatalf("markers and noise must be excluded, got %+v", sliced)
	}

	rep, err := VerifyStatements(sliced)
	if err != nil || rep.WriteUnits != 1 {
		t.Fatalf("expected 1 write unit inside the markers, got %+v (err %v)", rep, err)
	}
}

func TestScenarioMissingMarkers(t *testing.T) {
	noMarkers := []Statement{write("t1", "INSERT INTO users (id) VALUES (1)", 1)}
	if _, err := Scenario(noMarkers, "create-user"); err == nil ||
		!strings.Contains(err.Error(), "begin marker") {
		t.Fatalf("expected begin-marker error, got %v", err)
	}

	truncated := []Statement{
		read("t1", BeginMarker("create-user"), 1),
		write("t2", "INSERT INTO users (id) VALUES (1)", 2),
	}
	if _, err := Scenario(truncated, "create-user"); err == nil ||
		!strings.Contains(err.Error(), "end marker") {
		t.Fatalf("expected end-marker error, got %v", err)
	}
}

// lineParser is a minimal Parser used to exercise the Verify entry points:
// each input line is "<txid> <sql>", with txid "-" meaning no identity.
type lineParser struct{}

func (lineParser) Parse(r io.Reader) ([]Statement, error) {
	var stmts []Statement
	sc := bufio.NewScanner(r)
	line := 0
	for sc.Scan() {
		line++
		txID, sql, ok := strings.Cut(sc.Text(), " ")
		if !ok {
			continue
		}
		if txID == "-" {
			txID = ""
		}
		stmts = append(stmts, Statement{
			SQL:  sql,
			Kind: txnproof.DefaultClassifier(sql),
			TxID: txID,
			Line: line,
		})
	}
	return stmts, sc.Err()
}

func TestVerifyWithParser(t *testing.T) {
	log := `t1 INSERT INTO a (id) VALUES (1)
t2 INSERT INTO b (id) VALUES (1)
`
	rep, err := Verify(lineParser{}, strings.NewReader(log))
	var nae *NonAtomicError
	if !errors.As(err, &nae) {
		t.Fatalf("expected *NonAtomicError, got %v", err)
	}
	if rep.WriteUnits != 2 {
		t.Fatalf("expected 2 write units, got %+v", rep)
	}
}

func TestVerifyScenarioWithParser(t *testing.T) {
	log := "t0 INSERT INTO noise (id) VALUES (1)\n" +
		"t1 " + BeginMarker("s") + "\n" +
		"t2 INSERT INTO a (id) VALUES (1)\n" +
		"t2 UPDATE a SET n = 1\n" +
		"t3 " + EndMarker("s") + "\n" +
		"t4 INSERT INTO noise (id) VALUES (2)\n"
	rep, err := VerifyScenario(lineParser{}, strings.NewReader(log), "s")
	if err != nil {
		t.Fatalf("expected atomic scenario, got %v", err)
	}
	if rep.WriteUnits != 1 || rep.Units[0].TxID != "t2" {
		t.Fatalf("noise outside the markers must be excluded, got %+v", rep)
	}

	// Without slicing, the noise writes make the same log non-atomic.
	if _, err := Verify(lineParser{}, strings.NewReader(log)); err == nil {
		t.Fatal("whole log should not be atomic")
	}
}
