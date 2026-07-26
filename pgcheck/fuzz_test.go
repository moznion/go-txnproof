package pgcheck

// Fuzz targets for the PostgreSQL adapter. Parse consumes a real server's log
// file — content produced by a process the test does not control, in a format
// that varies with the server version, the locale and whatever else writes to
// the same log — so it must stay a total function: any bytes must yield
// statements or an error, never a panic and never a fabricated transaction
// identity.

import (
	"bufio"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/moznion/go-txnproof"
	"github.com/moznion/go-txnproof/crosscheck"
)

// fuzzLogSeeds are real-shaped log excerpts: the default prefix, both query
// protocols, a continuation line, a background-process line, and the "no
// virtual transaction" renderings that must never become an identity.
var fuzzLogSeeds = []string{
	"2026-07-18 01:58:59.266 UTC [123] 0 3/42 LOG:  statement: INSERT INTO t VALUES (1)\n",
	"2026-07-18 01:58:59.266 UTC [123] 812 3/42 LOG:  execute stmt1: UPDATE t SET a = 1\n",
	"2026-07-18 01:58:59.266 UTC [123] 0 0/0 LOG:  statement: SELECT 1\n",
	"2026-07-18 01:58:59.266 UTC [123] 0 3/0 LOG:  statement: SELECT 1\n",
	"2026-07-18 01:58:59.266 UTC [123] 0 3/42 LOG:  statement: INSERT INTO t\n\tVALUES (1)\n",
	"2026-07-18 01:58:59.266 UTC [123] 0 3/42 LOG:  duration: 1.234 ms  statement: DELETE FROM t\n",
	"2026-07-18 01:58:59.266 UTC [123] LOG:  checkpoint starting: time\n",
	"2026-07-18 01:58:59.266 UTC [123] 0 3/42 LOG:  parse stmt1: INSERT INTO t VALUES (1)\n",
	"",
	"\t\n\n\t\n",
}

// FuzzParse pins the parser's contract on arbitrary log content: line numbers
// stay meaningful, classifications stay in range, parsing is deterministic,
// and — the regression that matters most — a ".../0" virtual transaction id is
// never accepted as an identity, which would silently merge unrelated
// auto-commit writes into one fake transaction.
func FuzzParse(f *testing.F) {
	for _, s := range fuzzLogSeeds {
		f.Add(s)
	}
	f.Add(strings.Join(fuzzLogSeeds, ""))

	c, err := New()
	if err != nil {
		f.Fatalf("New: %v", err)
	}

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
			if strings.HasSuffix(st.TxID, "/0") {
				t.Fatalf("statement %d took %q as a transaction identity; a local transaction id of 0 means no virtual transaction", i, st.TxID)
			}
			if st.TxID == "xid 0" {
				t.Fatalf("statement %d took %q as a transaction identity; xid 0 means none was assigned", i, st.TxID)
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

		// The verification path on top of it must reach a verdict or a typed
		// error, never a panic.
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
			// A write the server logged without any transaction identity.
		default:
			t.Fatalf("Verify returned an unexpected error: %v", err)
		}
	})
}

// FuzzVerifyScenario runs the marker-delimited path over arbitrary logs and
// labels: a log that does not delimit the scenario must produce an error, not
// a wrong verdict or a crash.
func FuzzVerifyScenario(f *testing.F) {
	prefix := "2026-07-18 01:58:59.266 UTC [123] 0 3/42 LOG:  statement: "
	f.Add("scenario", prefix+BeginMarker("scenario")+"\n"+prefix+"INSERT INTO t VALUES (1)\n"+prefix+EndMarker("scenario")+"\n")
	f.Add("scenario", prefix+"INSERT INTO t VALUES (1)\n")
	f.Add("", "")

	c, err := New()
	if err != nil {
		f.Fatalf("New: %v", err)
	}

	f.Fuzz(func(t *testing.T, label, log string) {
		rep, err := c.VerifyScenario(strings.NewReader(log), label)
		if err == nil && !rep.Atomic() {
			t.Fatalf("VerifyScenario succeeded with a non-atomic report: %+v", rep)
		}
		if err != nil && rep != nil && rep.Atomic() {
			t.Fatalf("VerifyScenario failed with an atomic report: %v / %+v", err, rep)
		}
	})
}

// FuzzCompileLogLinePrefix feeds arbitrary log_line_prefix values to the
// translator: a prefix it cannot handle must be rejected with an error, and a
// prefix it accepts must produce a usable, anchored pattern — never a panic
// and never a pattern regexp.Compile chokes on.
func FuzzCompileLogLinePrefix(f *testing.F) {
	for _, p := range []string{
		DefaultLogLinePrefix,
		"%m [%p] %q%x %v ",
		"%t %c %e %x ",
		"%a%u%d%r%h%b%i %v ",
		"%%x %v ",
		"%",
		"%z",
		"",
		"%v%x",
	} {
		f.Add(p)
	}

	f.Fuzz(func(t *testing.T, prefix string) {
		re, err := CompileLogLinePrefix(prefix)
		if err != nil {
			if re != nil {
				t.Fatalf("CompileLogLinePrefix returned a pattern alongside an error: %v", err)
			}
			if !strings.Contains(err.Error(), "pgcheck: ") {
				t.Fatalf("error is not attributed to the package: %v", err)
			}
			return
		}
		if re == nil {
			t.Fatal("CompileLogLinePrefix returned no pattern and no error")
		}
		if !strings.HasPrefix(re.String(), "^") {
			t.Fatalf("pattern %q is not anchored at the start of the line", re)
		}
		// Matching arbitrary input against it must be safe.
		re.FindStringSubmatchIndex("2026-07-18 01:58:59.266 UTC [123] 0 3/42 LOG:  statement: SELECT 1")

		// New accepts the prefix exactly when the pattern carries at least one
		// transaction identifier group.
		usable := hasGroup(re, "xid") || hasGroup(re, "vxid")
		checker, err := New(WithLogLinePrefix(prefix))
		if usable != (err == nil) {
			t.Fatalf("New(WithLogLinePrefix(%q)) error = %v, but the pattern %q captures xid/vxid = %v", prefix, err, re, usable)
		}
		if err != nil {
			return
		}
		// A checker built from an accepted prefix must parse without panicking.
		if _, err := checker.Parse(strings.NewReader(strings.Join(fuzzLogSeeds, ""))); err != nil {
			t.Fatalf("Parse with prefix %q failed: %v", prefix, err)
		}
	})
}

// FuzzPrefixPattern covers the hand-written-pattern escape hatch: any regexp
// the caller supplies must either be rejected by New or drive Parse safely.
func FuzzPrefixPattern(f *testing.F) {
	f.Add(`^(?P<vxid>\d+/\d+) `, "3/42 LOG:  statement: INSERT INTO t VALUES (1)\n")
	f.Add(`^(?P<xid>\d+) `, "812 LOG:  statement: INSERT INTO t VALUES (1)\n")
	f.Add(`^`, "LOG:  statement: SELECT 1\n")
	f.Add(`(?P<vxid>.*)`, "")

	f.Fuzz(func(t *testing.T, pattern, log string) {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return // not a pattern a caller could have passed in
		}
		c, err := New(WithPrefixPattern(re))
		if err != nil {
			if hasGroup(re, "xid") || hasGroup(re, "vxid") {
				t.Fatalf("New rejected the usable pattern %q: %v", pattern, err)
			}
			return
		}
		stmts, err := c.Parse(strings.NewReader(log))
		if err != nil {
			if !errors.Is(err, bufio.ErrTooLong) {
				t.Fatalf("Parse with pattern %q returned an unexpected error: %v", pattern, err)
			}
			return
		}
		for i, st := range stmts {
			if strings.HasSuffix(st.TxID, "/0") {
				t.Fatalf("statement %d took %q as a transaction identity", i, st.TxID)
			}
		}
	})
}

func isNonAtomic(err error) bool {
	var e *crosscheck.NonAtomicError
	return errors.As(err, &e)
}

func isMissingTxID(err error) bool {
	var e *crosscheck.MissingTxIDError
	return errors.As(err, &e)
}
