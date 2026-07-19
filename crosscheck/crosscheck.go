// Package crosscheck verifies a scenario's atomicity from a database
// server's own record of execution: given the statements the server logged
// while a test scenario ran, each annotated with the server-side transaction
// it ran in, it checks that all write statements shared one transaction.
//
// The root txnproof package observes statements on the client side, which has
// known blind spots: statements executed with a detached context, heuristic
// statement classification, and best-effort tracking of textual
// BEGIN/COMMIT. crosscheck complements it with server-side truth for tests
// that run against a real database — the server's log is the authoritative
// record of which transaction each statement actually ran in.
//
// crosscheck itself is database-agnostic. Everything specific to one
// database lives in an adapter package built on top of it, such as the
// sibling pgcheck package for PostgreSQL.
//
// # Adapter contract
//
// An adapter owns exactly one job: turning a database-specific log (or any
// other server-side record of executed statements) into a []Statement. For
// each statement it must fill in
//
//   - SQL: the statement text,
//   - Kind: its classification (adapters reuse txnproof.DefaultClassifier
//     unless configured otherwise),
//   - TxID: an opaque, non-empty token identifying the server-side
//     transaction the statement ran in — equal for two statements exactly
//     when the server ran them in the same transaction. The format is the
//     adapter's choice (crosscheck only compares TxIDs for equality and
//     prints them in messages), so it should be short and human-meaningful.
//   - Line: the 1-based line number in the parsed source, for messages
//     (0 if the source has no line structure).
//
// The Parser interface captures this contract. Everything downstream —
// grouping writes into units, atomicity checking, marker-based scenario
// slicing, error rendering — is shared and lives in this package; an adapter
// typically wraps a Parser with database-specific configuration and
// delegates verification to Verify / VerifyScenario.
//
// A write with an empty TxID cannot be attributed to any transaction and
// makes the check fail with a *MissingTxIDError: guessing would silently
// produce wrong verdicts. Adapters should wrap that error with advice on how
// to configure the server so the identity becomes available. Reads may carry
// an empty TxID freely; they never count.
//
// # Semantics
//
// The verdict matches the root package's client-side semantics: reads never
// count; every server-side transaction that contained at least one write is
// one atomic unit — a rolled-back transaction included, since a rollback
// proves a partial-write path structurally exists; two or more units mean
// the scenario is not atomic and Verify / VerifyScenario return a
// *NonAtomicError listing the offending writes grouped by transaction.
//
// # Correlating log content with a scenario
//
// crosscheck cannot know by itself which logged statements belong to one
// scenario; the test has to delimit them. Two pragmatic mechanisms,
// combinable:
//
//   - Record the log file's size before the scenario and parse only the
//     bytes appended after it (the tail), then use Verify.
//   - Execute BeginMarker / EndMarker statements around the scenario —
//     harmless SELECTs whose recognizable literals delimit it in the log —
//     then use VerifyScenario. This is robust against interleaved noise
//     from other connections appearing before or after the scenario, but
//     not against unrelated statements interleaved inside it, so prefer a
//     dedicated database (or at least a quiet one) for such tests.
package crosscheck

import (
	"io"

	txnproof "github.com/moznion/go-txnproof"
)

// Statement is one SQL statement extracted from a database's server-side
// log, annotated with the transaction identity the server recorded for it.
// Adapters produce these; see the package documentation for the contract.
type Statement struct {
	// SQL is the statement text as logged.
	SQL string
	// Kind is the classification of SQL; only KindWrite statements count.
	Kind txnproof.StatementKind
	// TxID is the opaque server-side transaction identity assigned by the
	// adapter: non-empty, and equal for two statements exactly when the
	// server ran them in the same transaction. Empty means the adapter
	// could not attribute the statement to any transaction, which is a
	// *MissingTxIDError for writes at check time.
	TxID string
	// Line is the 1-based line number of the statement's first line in the
	// parsed source, used in messages; 0 if the source has no line
	// structure.
	Line int
}

// Parser is the adapter contract: it extracts the logged statements, with
// per-statement transaction identity, from a database-specific log. See the
// package documentation for what an implementation must guarantee.
type Parser interface {
	Parse(r io.Reader) ([]Statement, error)
}

// Verify parses the given log content with the adapter and checks that all
// writes in it shared one server-side transaction. The returned Report is
// non-nil whenever parsing and checking succeeded, so the caller can inspect
// it even on failure; the error is a *NonAtomicError when the writes span
// two or more transactions.
//
// Feed it only the log content produced by the scenario — typically the file
// tail appended after an offset recorded before the scenario ran (see the
// package documentation). For marker-delimited scenarios use VerifyScenario.
func Verify(p Parser, r io.Reader) (*Report, error) {
	stmts, err := p.Parse(r)
	if err != nil {
		return nil, err
	}
	return VerifyStatements(stmts)
}

// VerifyScenario parses the given log content with the adapter, slices out
// the scenario delimited by BeginMarker / EndMarker statements for the
// label, and checks that all writes in it shared one server-side
// transaction. The returned Report is non-nil whenever parsing, slicing, and
// checking succeeded; the error is a *NonAtomicError when the writes span
// two or more transactions.
func VerifyScenario(p Parser, r io.Reader, label string) (*Report, error) {
	stmts, err := p.Parse(r)
	if err != nil {
		return nil, err
	}
	stmts, err = Scenario(stmts, label)
	if err != nil {
		return nil, err
	}
	return VerifyStatements(stmts)
}

// VerifyStatements checks that all writes among stmts shared one server-side
// transaction: it runs Check and turns a non-atomic Report into a
// *NonAtomicError (returned alongside the Report so the caller can inspect
// it). Use it directly when the statements come from something other than an
// io.Reader-based Parser.
func VerifyStatements(stmts []Statement) (*Report, error) {
	rep, err := Check(stmts)
	if err != nil {
		return nil, err
	}
	if !rep.Atomic() {
		return rep, &NonAtomicError{Report: rep}
	}
	return rep, nil
}
