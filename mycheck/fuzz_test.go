package mycheck

// Fuzz targets for the MySQL adapter. Unlike pgcheck, mycheck does not read a
// transaction identity off the log line — it reconstructs the grouping with a
// state machine — so arbitrary general-log content drives that state machine
// into states real logs rarely reach. Both the parser and the state machine
// must stay total: any bytes yield statements or an error, never a panic and
// never a statement without an identity.

import (
	"bufio"
	"errors"
	"strings"
	"testing"

	"github.com/moznion/go-txnproof"
	"github.com/moznion/go-txnproof/crosscheck"
)

// fuzzLogSeeds are real-shaped general-log excerpts: the opening banner, the
// column header, both timestamp renderings, the commands that do and do not
// carry a statement, a continuation line, and a Quit mid-transaction.
var fuzzLogSeeds = []string{
	"/usr/sbin/mysqld, Version: 9.7.0 (MySQL Community Server - GPL). started with:\n" +
		"Tcp port: 3306  Unix socket: /tmp/mysql.sock\n" +
		"Time                 Id Command    Argument\n",
	"2026-07-18T01:58:59.266281Z\t   11 Query\tBEGIN\n",
	"2026-07-18T01:58:59.266281Z\t   11 Query\tINSERT INTO t VALUES (1)\n",
	"2026-07-18T01:58:59.266281Z\t   11 Query\tCOMMIT\n",
	"2026-07-18T01:58:59.266281+09:00\t   11 Query\tINSERT INTO t VALUES (1)\n",
	"2026-07-18T01:58:59.266281Z\t   11 Prepare\tINSERT INTO t VALUES (?)\n" +
		"2026-07-18T01:58:59.266281Z\t   11 Execute\tINSERT INTO t VALUES (1)\n",
	"2026-07-18T01:58:59.266281Z\t   11 Query\tINSERT INTO t\nVALUES (1)\n",
	"2026-07-18T01:58:59.266281Z\t   11 Query\tCREATE TABLE t2 (id int)\n",
	"2026-07-18T01:58:59.266281Z\t   11 Query\tROLLBACK TO SAVEPOINT sp\n",
	"2026-07-18T01:58:59.266281Z\t   11 Quit\t\n",
	"2026-07-18T01:58:59.266281Z\t   12 Connect\troot@localhost on test\n",
	"2026-07-18T01:58:59.266281Z\t   11 Query\tSET autocommit = 0\n",
	"",
}

// FuzzParse pins the reconstruction contract on arbitrary log content: line
// numbers stay meaningful, classifications stay in range, parsing is
// deterministic, and — the invariant the package documentation promises —
// every statement Parse returns carries a synthesized transaction identity, so
// a MissingTxIDError can never come out of the parse path.
func FuzzParse(f *testing.F) {
	for _, s := range fuzzLogSeeds {
		f.Add(s)
	}
	f.Add(strings.Join(fuzzLogSeeds, ""))

	c := New()

	f.Fuzz(func(t *testing.T, log string) {
		stmts, err := c.Parse(strings.NewReader(log))
		if err != nil {
			// The only failure a reader over a string can produce is an
			// over-long line hitting the scanner's buffer cap.
			if !errors.Is(err, bufio.ErrTooLong) {
				t.Fatalf("Parse returned an unexpected error: %v", err)
			}
			return
		}

		lines := strings.Count(log, "\n") + 1
		prev := 0
		for i, st := range stmts {
			if st.TxID == "" {
				t.Fatalf("statement %d (%q) has no transaction identity; mycheck must synthesize one for every parsed statement", i, st.SQL)
			}
			if st.Line < 1 || st.Line > lines {
				t.Fatalf("statement %d has line %d, outside 1..%d", i, st.Line, lines)
			}
			if st.Line <= prev {
				t.Fatalf("statement %d has line %d after line %d; statements must follow the log order", i, st.Line, prev)
			}
			prev = st.Line
			if st.Kind < txnproof.KindOther || st.Kind > txnproof.KindRollback {
				t.Fatalf("statement %d has kind %d, outside the defined range", i, st.Kind)
			}
		}

		// Parsing must be deterministic: the same log yields the same statements.
		again, err := c.Parse(strings.NewReader(log))
		if err != nil {
			t.Fatalf("second Parse failed after the first succeeded: %v", err)
		}
		if len(again) != len(stmts) {
			t.Fatalf("Parse is not deterministic: %d then %d statements", len(stmts), len(again))
		}
		for i := range stmts {
			if again[i] != stmts[i] {
				t.Fatalf("Parse is not deterministic at statement %d: %+v then %+v", i, stmts[i], again[i])
			}
		}

		rep, err := c.Verify(strings.NewReader(log))
		switch {
		case err == nil:
			if !rep.Atomic() {
				t.Fatalf("Verify succeeded with a non-atomic report: %+v", rep)
			}
		case isNonAtomic(err):
			if rep == nil || rep.Atomic() {
				t.Fatalf("Verify reported non-atomic execution with report %+v", rep)
			}
		case isMissingTxID(err):
			t.Fatalf("Verify hit a missing transaction identity on its own parse output: %v", err)
		case strings.Contains(err.Error(), "autocommit"):
			// An autocommit-mode change inside the verified slice is a hard,
			// deliberate error: the grouping rules would be wrong.
		default:
			t.Fatalf("Verify returned an unexpected error: %v", err)
		}
	})
}

// FuzzVerifyScenario runs the marker-delimited path over arbitrary logs and
// labels: a log that does not delimit the scenario must produce an error, not
// a wrong verdict or a crash.
func FuzzVerifyScenario(f *testing.F) {
	entry := func(sql string) string {
		return "2026-07-18T01:58:59.266281Z\t   11 Query\t" + sql + "\n"
	}
	f.Add("scenario", entry(BeginMarker("scenario"))+entry("INSERT INTO t VALUES (1)")+entry(EndMarker("scenario")))
	f.Add("scenario", entry("INSERT INTO t VALUES (1)"))
	f.Add("", "")

	c := New()

	f.Fuzz(func(t *testing.T, label, log string) {
		rep, err := c.VerifyScenario(strings.NewReader(log), label)
		if err == nil && !rep.Atomic() {
			t.Fatalf("VerifyScenario succeeded with a non-atomic report: %+v", rep)
		}
		if err != nil {
			if rep != nil && rep.Atomic() {
				t.Fatalf("VerifyScenario failed with an atomic report: %v / %+v", err, rep)
			}
			if isMissingTxID(err) {
				t.Fatalf("VerifyScenario hit a missing transaction identity on its own parse output: %v", err)
			}
		}
	})
}

// FuzzThreadGrouping pins the guarantee that makes a synthesized identity
// usable at all: statements of different connections must never share one.
// The generated log interleaves entries from several threads, which is exactly
// the situation the per-thread state machine exists for.
func FuzzThreadGrouping(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5})
	f.Add([]byte{1, 1, 1, 1})
	f.Add([]byte{})

	// The statement each op logs. Transaction control and implicit-commit
	// statements are in there so the state machine actually changes state.
	sqls := []string{
		"BEGIN",
		"INSERT INTO t VALUES (1)",
		"COMMIT",
		"ROLLBACK",
		"SELECT 1",
		"CREATE TABLE t2 (id int)",
		"ROLLBACK TO SAVEPOINT sp",
		"START TRANSACTION",
	}
	commands := []string{"Query", "Execute", "Prepare", "Quit", "Connect"}
	c := New()

	f.Fuzz(func(t *testing.T, program []byte) {
		if len(program) > 128 {
			program = program[:128]
		}
		var log strings.Builder
		threadOf := map[int]string{} // log line -> thread that produced it
		for i, b := range program {
			thread := int(b/32)%3 + 10
			cmd := commands[int(b/8)%len(commands)]
			sql := sqls[int(b)%len(sqls)]
			log.WriteString("2026-07-18T01:58:59.26628" + string(rune('0'+i%10)) + "Z\t   ")
			log.WriteString(itoa(thread))
			log.WriteString(" " + cmd + "\t" + sql + "\n")
			threadOf[i+1] = itoa(thread)
		}

		stmts, err := c.Parse(strings.NewReader(log.String()))
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		owner := map[string]string{} // TxID -> thread it belongs to
		for _, st := range stmts {
			thread, ok := threadOf[st.Line]
			if !ok {
				t.Fatalf("statement at line %d does not correspond to a generated entry", st.Line)
			}
			if prev, ok := owner[st.TxID]; ok && prev != thread {
				t.Fatalf("transaction identity %q is shared by threads %s and %s", st.TxID, prev, thread)
			}
			owner[st.TxID] = thread
		}
	})
}

// itoa avoids pulling strconv in just for thread ids.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

func isNonAtomic(err error) bool {
	var e *crosscheck.NonAtomicError
	return errors.As(err, &e)
}

func isMissingTxID(err error) bool {
	var e *crosscheck.MissingTxIDError
	return errors.As(err, &e)
}
