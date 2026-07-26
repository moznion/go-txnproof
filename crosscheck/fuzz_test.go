package crosscheck

// Fuzz targets for the database-agnostic core. Check and Scenario consume
// whatever an adapter parsed out of a server log — that is, content the test
// author does not control — so both must be total functions of their input:
// any statement list must produce a verdict or an error, never a panic.

import (
	"errors"
	"strings"
	"testing"

	"github.com/moznion/go-txnproof"
)

// fuzzKinds is the classification each generated statement can carry, covering
// the whole StatementKind range so that the "only writes count" rule is
// exercised from every direction.
var fuzzKinds = []txnproof.StatementKind{
	txnproof.KindOther,
	txnproof.KindRead,
	txnproof.KindWrite,
	txnproof.KindBegin,
	txnproof.KindCommit,
	txnproof.KindRollback,
}

// fuzzTxIDs includes the empty identity: a write carrying it is a hard error,
// a read carrying it is fine.
var fuzzTxIDs = []string{"", "t1", "t2", "t3"}

// statementsFrom decodes a byte program into statements: two bits pick the
// transaction identity, the rest the kind. Line numbers are 1-based positions
// so that assertions can identify a statement by its line.
func statementsFrom(program []byte) []Statement {
	stmts := make([]Statement, 0, len(program))
	for i, b := range program {
		stmts = append(stmts, Statement{
			SQL:  "stmt " + string(rune('a'+int(b)%26)),
			Kind: fuzzKinds[int(b)%len(fuzzKinds)],
			TxID: fuzzTxIDs[int(b/8)%len(fuzzTxIDs)],
			Line: i + 1,
		})
	}
	return stmts
}

// FuzzCheck pins the grouping semantics: one unit per distinct transaction
// that contained a write, reads never counted, and an unattributable write
// rejected outright rather than guessed at.
func FuzzCheck(f *testing.F) {
	f.Add([]byte{2, 2})
	f.Add([]byte{2, 10})
	f.Add([]byte{1, 2, 3, 4, 5})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, program []byte) {
		if len(program) > 256 {
			program = program[:256]
		}
		stmts := statementsFrom(program)

		rep, err := Check(stmts)
		if err != nil {
			var missing *MissingTxIDError
			if !errors.As(err, &missing) {
				t.Fatalf("Check returned an unexpected error type %T: %v", err, err)
			}
			if missing.Statement.Kind != txnproof.KindWrite || missing.Statement.TxID != "" {
				t.Fatalf("MissingTxIDError names %+v, want a write with an empty TxID", missing.Statement)
			}
			if missing.Error() == "" {
				t.Fatal("MissingTxIDError renders as an empty string")
			}
			return
		}

		// No error means no write carried an empty identity.
		wantUnits := map[string]int{}
		var order []string
		for _, st := range stmts {
			if st.Kind != txnproof.KindWrite {
				continue
			}
			if st.TxID == "" {
				t.Fatalf("Check accepted a write with an empty TxID at line %d", st.Line)
			}
			if _, ok := wantUnits[st.TxID]; !ok {
				order = append(order, st.TxID)
			}
			wantUnits[st.TxID]++
		}

		if rep.WriteUnits != len(order) || len(rep.Units) != len(order) {
			t.Fatalf("report has %d write units / %d units, want %d", rep.WriteUnits, len(rep.Units), len(order))
		}
		if rep.Atomic() != (rep.WriteUnits <= 1) {
			t.Fatalf("Atomic() = %v with %d write units", rep.Atomic(), rep.WriteUnits)
		}
		for i, u := range rep.Units {
			if u.TxID != order[i] {
				t.Fatalf("unit %d is %q, want %q (units must follow first appearance)", i, u.TxID, order[i])
			}
			if len(u.Statements) != wantUnits[u.TxID] {
				t.Fatalf("unit %q holds %d statement(s), want %d", u.TxID, len(u.Statements), wantUnits[u.TxID])
			}
			for _, st := range u.Statements {
				if st.Kind != txnproof.KindWrite {
					t.Fatalf("unit %q holds a non-write statement: %+v", u.TxID, st)
				}
			}
		}
		if rep.String() == "" {
			t.Fatal("Report.String() is empty")
		}

		// VerifyStatements must agree with the report it returns.
		verified, err := VerifyStatements(stmts)
		if err != nil {
			var na *NonAtomicError
			if !errors.As(err, &na) {
				t.Fatalf("VerifyStatements returned an unexpected error type %T: %v", err, err)
			}
			if verified.Atomic() {
				t.Fatalf("VerifyStatements failed with an atomic report: %+v", verified)
			}
			if na.Error() == "" {
				t.Fatal("NonAtomicError renders as an empty string")
			}
			return
		}
		if !verified.Atomic() {
			t.Fatalf("VerifyStatements succeeded with a non-atomic report: %+v", verified)
		}
	})
}

// FuzzScenario checks the marker slicing on arbitrary logs and labels: the
// result must always be a contiguous run of input statements that starts after
// a begin marker and stops before the first following end marker.
func FuzzScenario(f *testing.F) {
	f.Add("scenario", []byte{0, 1, 2})
	f.Add("", []byte{})
	f.Add("'", []byte{2, 2})
	f.Add("scenario", []byte{200, 201, 2, 202})

	f.Fuzz(func(t *testing.T, label string, program []byte) {
		if len(program) > 128 {
			program = program[:128]
		}
		stmts := statementsFrom(program)
		// Give some statements a chance to carry a marker literal so that
		// slicing is actually reachable from the generated program.
		for i := range stmts {
			switch program[i] % 5 {
			case 3:
				stmts[i].SQL = BeginMarker(label)
			case 4:
				stmts[i].SQL = EndMarker(label)
			}
		}

		got, err := Scenario(stmts, label)
		if err != nil {
			if got != nil {
				t.Fatalf("Scenario returned %d statement(s) alongside an error", len(got))
			}
			if !strings.Contains(err.Error(), "crosscheck: ") {
				t.Fatalf("Scenario error is not attributed to the package: %v", err)
			}
			return
		}

		endLit := markerLiteral("end", label)
		for i, st := range got {
			if strings.Contains(st.SQL, endLit) {
				t.Fatalf("sliced statement %d still contains the end marker: %q", i, st.SQL)
			}
			// Line numbers are positions in the input, so a contiguous slice
			// must have consecutive lines starting after the begin marker.
			if want := got[0].Line + i; st.Line != want {
				t.Fatalf("sliced statement %d has line %d, want %d (the slice must be contiguous)", i, st.Line, want)
			}
		}
		if len(got) > 0 && got[0].Line < 2 {
			t.Fatalf("slice starts at line %d, but it must start after the begin marker", got[0].Line)
		}
	})
}

// FuzzScenarioRoundTrip is the property the marker mechanism promises to its
// users: wrap the scenario's statements in BeginMarker/EndMarker for a label —
// any label, including one with quotes — and Scenario hands exactly those
// statements back.
func FuzzScenarioRoundTrip(f *testing.F) {
	f.Add("scenario", []byte{1, 2, 3})
	f.Add("with 'quotes'", []byte{2})
	f.Add("", []byte{})

	f.Fuzz(func(t *testing.T, label string, program []byte) {
		if len(program) > 128 {
			program = program[:128]
		}
		beginLit := markerLiteral("begin", label)
		endLit := markerLiteral("end", label)
		// A label crafted so that one marker contains the other's literal makes
		// the delimiters ambiguous; the mechanism never promised to survive
		// that, so those labels are out of scope.
		if strings.Contains(BeginMarker(label), endLit) || strings.Contains(EndMarker(label), beginLit) {
			return
		}

		inner := statementsFrom(program)
		for _, st := range inner {
			if strings.Contains(st.SQL, beginLit) || strings.Contains(st.SQL, endLit) {
				return // an inner statement impersonating a marker is ambiguous too
			}
		}

		stmts := append([]Statement{{SQL: BeginMarker(label), Kind: txnproof.KindRead, TxID: "m", Line: 0}}, inner...)
		stmts = append(stmts, Statement{SQL: EndMarker(label), Kind: txnproof.KindRead, TxID: "m", Line: len(stmts)})

		got, err := Scenario(stmts, label)
		if err != nil {
			t.Fatalf("Scenario(%q) failed on its own markers: %v", label, err)
		}
		if len(got) != len(inner) {
			t.Fatalf("Scenario returned %d statement(s), want the %d wrapped ones", len(got), len(inner))
		}
		for i := range got {
			if got[i] != inner[i] {
				t.Fatalf("statement %d came back as %+v, want %+v", i, got[i], inner[i])
			}
		}
	})
}
