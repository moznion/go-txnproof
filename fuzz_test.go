package txnproof

// Fuzz targets for the root package. txnproof sits in its host application's
// request path as driver middleware, so a panic here takes the application
// down with it — a far worse outcome than a missed violation. Every function
// that touches caller-controlled text (SQL queries, boundary names, baseline
// files) is fuzzed for crash-freedom, and where the behavior is specified
// exactly (throttle key derivation, baseline round-trip) the target asserts
// that specification rather than merely "did not panic".

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// commentPrefixes are prefixes DefaultClassifier must strip before it looks at
// the leading keyword. None of them contains a write keyword, which matters:
// the data-modifying-CTE scan looks at the whole query text, so a prefix
// carrying e.g. INSERT would legitimately change a WITH statement's verdict.
var commentPrefixes = []string{
	" ",
	"\t\r\n ",
	"/* c */",
	"/* c */ ",
	"-- c\n",
	"/* multi\nline */\n-- another\n  ",
}

// FuzzDefaultClassifier pins the classifier's total contract: it must never
// panic on arbitrary text, must return a defined StatementKind, must be a pure
// function (the prepared-statement path classifies once at Prepare and reuses
// the result forever, so an unstable classifier would silently mis-count), and
// must be blind to leading whitespace/comments and — for ASCII input — to
// letter case.
func FuzzDefaultClassifier(f *testing.F) {
	for _, q := range []string{
		"",
		" ",
		"SELECT 1",
		"insert into t values (1)",
		"WITH x AS (SELECT 1) SELECT * FROM x",
		"WITH moved AS (DELETE FROM t RETURNING *) INSERT INTO archive SELECT * FROM moved",
		"BEGIN",
		"COMMIT",
		"ROLLBACK",
		"ROLLBACK TO SAVEPOINT sp1",
		"-- comment\nSELECT 1",
		"/* unterminated",
		"--",
		"/*",
		"*/ SELECT 1",
		strings.Repeat("A", 64) + " INSERT",
		"ＳＥＬＥＣＴ 1",
		"\x00\x01\x02",
	} {
		f.Add(q)
	}

	f.Fuzz(func(t *testing.T, query string) {
		kind := DefaultClassifier(query)
		if kind < KindOther || kind > KindRollback {
			t.Fatalf("DefaultClassifier(%q) = %d, outside the defined StatementKind range", query, kind)
		}
		if kind.String() == "" {
			t.Fatalf("StatementKind(%d).String() is empty", kind)
		}
		if again := DefaultClassifier(query); again != kind {
			t.Fatalf("DefaultClassifier(%q) is not pure: %v then %v", query, kind, again)
		}

		for _, prefix := range commentPrefixes {
			if got := DefaultClassifier(prefix + query); got != kind {
				t.Fatalf("DefaultClassifier(%q+%q) = %v, want %v (leading whitespace/comments must be stripped)", prefix, query, got, kind)
			}
		}

		// Case folding is only guaranteed for ASCII: Unicode case mapping can
		// turn a non-identifier rune into an ASCII letter (U+0131 uppercases
		// to 'I'), which legitimately changes the leading token.
		if isASCII(query) {
			if got := DefaultClassifier(strings.ToUpper(query)); got != kind {
				t.Fatalf("DefaultClassifier(upper %q) = %v, want %v", query, got, kind)
			}
			if got := DefaultClassifier(strings.ToLower(query)); got != kind {
				t.Fatalf("DefaultClassifier(lower %q) = %v, want %v", query, got, kind)
			}
		}
	})
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

// FuzzUnboundedWriteKey generalizes
// TestUnboundedWriteKeyMatchesNormalizeThenTruncate: the streaming key
// derivation exists only to make the cost bounded by the key length instead of
// the statement length, so it must stay byte-identical to the obvious
// normalize-then-truncate implementation for every input — invalid UTF-8,
// Unicode whitespace and truncation points splitting a rune included.
func FuzzUnboundedWriteKey(f *testing.F) {
	for _, q := range []string{
		"",
		" \t\r\n \v\f ",
		"INSERT INTO t VALUES (1)",
		"  INSERT\t\tINTO t\n VALUES  (1) \n",
		"INSERT　INTO t VALUES (1)",
		strings.Repeat("a", 3*unboundedWriteKeyLen),
		strings.Repeat("ab ", 2*unboundedWriteKeyLen),
		strings.Repeat("あ", unboundedWriteKeyLen),
		strings.Repeat("x", unboundedWriteKeyLen) + "   \n",
		"\xff\xfe INSERT \xff",
	} {
		f.Add(q)
	}

	f.Fuzz(func(t *testing.T, query string) {
		got := unboundedWriteKey(query)
		want := strings.Join(strings.Fields(query), " ")
		if len(want) > unboundedWriteKeyLen {
			want = want[:unboundedWriteKeyLen]
		}
		if got != want {
			t.Fatalf("unboundedWriteKey(%q) = %q, want %q", query, got, want)
		}
		if len(got) > unboundedWriteKeyLen {
			t.Fatalf("unboundedWriteKey(%q) is %d bytes, over the %d cap", query, len(got), unboundedWriteKeyLen)
		}
	})
}

// FuzzThrottlingReporterAccounting checks the throttle's central promise: no
// report is ever lost silently. Every submitted report must either reach the
// wrapped reporter or be counted as suppressed — including on the fail-open
// path past the statement-key cap, where a report is forwarded without being
// tracked.
func FuzzThrottlingReporterAccounting(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	f.Add([]byte{0, 0, 0, 8, 0, 0})
	f.Add([]byte{4, 4, 4, 4, 8, 4})

	f.Fuzz(func(t *testing.T, program []byte) {
		if len(program) > 256 {
			program = program[:256]
		}
		tr, cr, clock := setupThrottle(time.Minute)
		ctx := context.Background()

		var violations, unbounded, staleAllows, nested int
		for _, b := range program {
			key := string(rune('A' + b/8%4))
			switch b % 8 {
			case 0, 1:
				tr.Report(ctx, Violation{Boundary: key, WriteUnits: 2})
				violations++
			case 2, 3:
				tr.ReportUnboundedWrite(ctx, StatementRecord{Query: "INSERT INTO " + key + " VALUES (1)", Kind: KindWrite})
				unbounded++
			case 4:
				tr.ReportStaleAllow(ctx, StaleAllow{Boundary: key, Reason: "fuzz"})
				staleAllows++
			case 5:
				tr.ReportNestedBoundary(ctx, NestedBoundary{Outer: key, Inner: "inner"})
				nested++
			case 6:
				clock.Advance(time.Second)
			case 7:
				clock.Advance(time.Hour)
			}
		}

		for _, c := range []struct {
			name             string
			submitted        int
			forwarded        int
			suppressedCounts map[string]int
		}{
			{"violations", violations, len(cr.Violations()), tr.SuppressedViolations()},
			{"unbounded writes", unbounded, len(cr.UnboundedWrites()), tr.SuppressedUnboundedWrites()},
			{"stale allows", staleAllows, len(cr.StaleAllows()), tr.SuppressedStaleAllows()},
			{"nested boundaries", nested, len(cr.NestedBoundaries()), tr.SuppressedNestedBoundaries()},
		} {
			total := c.forwarded
			for _, n := range c.suppressedCounts {
				total += n
			}
			if total != c.submitted {
				t.Fatalf("%s: %d submitted but %d forwarded + %d suppressed accounted for", c.name, c.submitted, c.forwarded, total-c.forwarded)
			}
		}
	})
}

// FuzzBaselineFile feeds arbitrary bytes to LoadBaseline: a corrupted or
// hand-edited baseline file must produce an error, never a panic. When the
// file does load, the ratchet's invariants must hold — boundaries come back
// sorted and deduplicated, and saving what was loaded reproduces it exactly
// (the file is meant to live in version control with clean diffs).
func FuzzBaselineFile(f *testing.F) {
	f.Add([]byte(`{"comment":"c","boundaries":["A","B"]}`))
	f.Add([]byte(`{"boundaries":["B","A","A"]}`))
	f.Add([]byte(`{"boundaries":null}`))
	f.Add([]byte(`{`))
	f.Add([]byte(``))
	f.Add([]byte(`[]`))
	f.Add([]byte(`{"boundaries":[""]}`))

	// One temp dir for the whole target: fuzz iterations run sequentially in a
	// worker process, and a fresh dir per iteration would dominate the runtime.
	dir := f.TempDir()

	f.Fuzz(func(t *testing.T, data []byte) {
		path := filepath.Join(dir, "baseline.json")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("write baseline file: %v", err)
		}
		b, err := LoadBaseline(path)
		if err != nil {
			return
		}
		names := b.Boundaries()
		assertSortedUnique(t, names)

		// Round-trip: everything that loads must save back into something that
		// loads identically.
		savePath := filepath.Join(dir, "baseline-saved.json")
		if err := b.Save(savePath); err != nil {
			t.Fatalf("Save: %v", err)
		}
		reloaded, err := LoadBaseline(savePath)
		if err != nil {
			t.Fatalf("LoadBaseline after Save: %v", err)
		}
		if got := reloaded.Boundaries(); !equalStrings(got, names) {
			t.Fatalf("baseline round-trip changed the entries: %q -> %q", names, got)
		}
	})
}

// FuzzBaselineRoundTrip drives the adoption flow (collect boundary names,
// Save, commit, LoadBaseline in CI) with arbitrary boundary names: whatever a
// boundary is called, the entries that come back must be the same set, sorted
// and deduplicated, and UnusedEntries must start out as all of them.
func FuzzBaselineRoundTrip(f *testing.F) {
	f.Add("CreateUser\nSyncOrders")
	f.Add("")
	f.Add("A\nA\nB")
	f.Add("with\nnewlines \" and 'quotes'\n\t")

	dir := f.TempDir()

	f.Fuzz(func(t *testing.T, joined string) {
		names := strings.Split(joined, "\n")
		b := NewBaseline()
		for _, name := range names {
			b.Add(name)
		}

		path := filepath.Join(dir, "baseline.json")
		if err := b.Save(path); err != nil {
			t.Fatalf("Save: %v", err)
		}
		loaded, err := LoadBaseline(path)
		if err != nil {
			t.Fatalf("LoadBaseline: %v", err)
		}
		got := loaded.Boundaries()
		assertSortedUnique(t, got)

		// JSON string encoding is lossy for invalid UTF-8 (each bad byte
		// becomes U+FFFD), so the identity assertion only applies to names that
		// survive an encoding round-trip. Boundary names are code-defined
		// identifiers, so this is a documented non-goal rather than a gap.
		if !utf8.ValidString(joined) {
			return
		}
		if want := b.Boundaries(); !equalStrings(got, want) {
			t.Fatalf("baseline round-trip changed the entries: %q -> %q", want, got)
		}
		if unused := loaded.UnusedEntries(); !equalStrings(unused, got) {
			t.Fatalf("freshly loaded baseline reports %q as used, want all of %q unused", unused, got)
		}
	})
}

// FuzzViolationRendering makes sure the human-facing renderings never panic on
// arbitrary content: a Violation carries raw SQL text and boundary names, and
// String is what test failures and log lines are built from.
func FuzzViolationRendering(f *testing.F) {
	f.Add("CreateUser", "INSERT INTO t VALUES (1)", uint64(0), 3)
	f.Add("", "", uint64(1), 0)
	f.Add("%s%d%!", "%v %q\n\t", uint64(7), 200)

	f.Fuzz(func(t *testing.T, boundary, query string, txID uint64, truncated int) {
		v := Violation{
			Boundary:            boundary,
			WriteUnits:          2,
			Statements:          []StatementRecord{{Query: query, Kind: KindWrite, TxID: txID}},
			TruncatedStatements: truncated,
			Attrs:               []BoundaryAttr{{Key: boundary, Value: query}},
		}
		if v.String() == "" {
			t.Fatalf("Violation.String() is empty for %+v", v)
		}
		if got := len(SlogAttrs(v.Attrs)); got != len(v.Attrs) {
			t.Fatalf("SlogAttrs dropped attrs: %d -> %d", len(v.Attrs), got)
		}
		// The slog reporter formats the same content through a different path.
		NewSlogReporter(discardLogger()).Report(context.Background(), v)
	})
}

// discardLogger is a logger that formats everything and writes it nowhere: it
// exercises the reporter's formatting path without flooding the fuzz output.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func assertSortedUnique(t *testing.T, names []string) {
	t.Helper()
	if !sort.StringsAreSorted(names) {
		t.Fatalf("entries are not sorted: %q", names)
	}
	for i := 1; i < len(names); i++ {
		if names[i] == names[i-1] {
			t.Fatalf("entries contain a duplicate: %q", names)
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
