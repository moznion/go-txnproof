package crosscheck

import (
	"fmt"
	"strings"

	"github.com/moznion/go-txnproof"
)

// Unit is one server-side transaction that contained at least one write.
type Unit struct {
	// TxID is the opaque transaction identity shared by the unit's
	// statements, as assigned by the adapter.
	TxID string
	// Statements are the write statements that ran in this transaction, in
	// input order.
	Statements []Statement
}

// Report is the server-side view of a scenario's writes: which transactions
// they ran in. Atomic execution means at most one unit.
type Report struct {
	// WriteUnits is the number of distinct server-side transactions that
	// contained writes; equal to len(Units).
	WriteUnits int
	// Units are the write-containing transactions in order of first
	// appearance in the input.
	Units []Unit
}

// Atomic reports whether all writes shared one server-side transaction (or
// there were no writes at all).
func (r *Report) Atomic() bool { return r.WriteUnits <= 1 }

// String renders a human-readable summary; for a non-atomic report it lists
// the write statements grouped by server-side transaction.
func (r *Report) String() string {
	if r.WriteUnits == 0 {
		return "crosscheck: no writes found in the server log"
	}
	if r.WriteUnits == 1 {
		return fmt.Sprintf("crosscheck: all %d write(s) shared one server-side transaction (%s)", len(r.Units[0].Statements), r.Units[0].TxID)
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "crosscheck: writes span %d server-side transactions (want 1) — the scenario is not atomic:", r.WriteUnits)
	for _, u := range r.Units {
		fmt.Fprintf(&sb, "\n  transaction %s:", u.TxID)
		for _, s := range u.Statements {
			fmt.Fprintf(&sb, "\n    %s%s", lineRef(s), compactSQL(s.SQL))
		}
	}
	return sb.String()
}

// NonAtomicError is returned by Verify / VerifyScenario / VerifyStatements
// when the writes span two or more server-side transactions.
type NonAtomicError struct {
	Report *Report
}

func (e *NonAtomicError) Error() string { return e.Report.String() }

// MissingTxIDError is returned by Check (and the Verify functions) when a
// write statement carries an empty TxID: the adapter could not attribute it
// to any server-side transaction, so no verdict can be trusted. Adapters
// should wrap it with database-specific advice on how to configure the
// server so the identity becomes available.
type MissingTxIDError struct {
	// Statement is the offending write.
	Statement Statement
}

func (e *MissingTxIDError) Error() string {
	return fmt.Sprintf("crosscheck: write %shas no server-side transaction identity, so it cannot be attributed to any transaction: %s",
		lineRef(e.Statement), compactSQL(e.Statement.SQL))
}

// Check groups the write statements among stmts by the server-side
// transaction they ran in (their TxID) and returns the resulting Report.
// Reads and transaction-control statements never count; a rolled-back
// transaction that contained a write still counts as a unit, matching the
// root package's semantics.
//
// It fails with a *MissingTxIDError if a write carries an empty TxID.
func Check(stmts []Statement) (*Report, error) {
	units := map[string]*Unit{}
	var order []string
	for _, st := range stmts {
		if st.Kind != txnproof.KindWrite {
			continue
		}
		if st.TxID == "" {
			return nil, &MissingTxIDError{Statement: st}
		}
		u := units[st.TxID]
		if u == nil {
			u = &Unit{TxID: st.TxID}
			units[st.TxID] = u
			order = append(order, st.TxID)
		}
		u.Statements = append(u.Statements, st)
	}
	rep := &Report{WriteUnits: len(order)}
	for _, k := range order {
		rep.Units = append(rep.Units, *units[k])
	}
	return rep, nil
}

// lineRef renders "line N: " for statements that have a line number, and
// nothing for sources without line structure.
func lineRef(s Statement) string {
	if s.Line <= 0 {
		return ""
	}
	return fmt.Sprintf("line %d: ", s.Line)
}

// compactSQL collapses whitespace runs so multi-line statements render on
// one message line.
func compactSQL(sql string) string {
	return strings.Join(strings.Fields(sql), " ")
}
