// Package mycheck is the MySQL adapter for the crosscheck package: it parses
// the general query log a MySQL server produced while a test scenario ran,
// reconstructs which transaction each logged statement ran in, and delegates
// to crosscheck to verify that all write statements shared one transaction.
//
// The root txnproof package observes statements on the client side, which has
// known blind spots: statements executed with a detached context, heuristic
// statement classification, and best-effort tracking of textual
// BEGIN/COMMIT. mycheck complements it with the server's own record for
// tests that run against a real MySQL instance.
//
// Everything database-agnostic — grouping writes into units, the verdict and
// its semantics, marker-based scenario slicing (BeginMarker / EndMarker, re-
// exported here for convenience), Report / NonAtomicError — lives in
// crosscheck; this package owns only the MySQL specifics described below.
// See the crosscheck package documentation for the adapter contract and the
// shared semantics.
//
// # A weaker guarantee than PostgreSQL's
//
// MySQL puts no transaction identifier on general-log lines — there is
// nothing like PostgreSQL's %v virtual transaction ID for pgcheck to read.
// mycheck therefore reconstructs the transaction grouping itself: every
// general-log entry carries the server's thread (connection) id, so the
// statement stream of each thread is replayed through a small state machine
// (BEGIN / START TRANSACTION opens a transaction, COMMIT / ROLLBACK closes
// it, everything in between belongs to it, and statements outside any
// transaction are their own auto-commit unit), and each group is given a
// synthesized opaque identity such as "thread 42 tx 3" or "thread 42 stmt
// 17".
//
// This is weaker than pgcheck's guarantee: the grouping is inferred from the
// server's statement stream, not read from a server-assigned transaction
// identifier. It is still server-side truth about which statements actually
// ran, in what order, on which connection — which is exactly what catches
// txnproof's client-side blind spots (detached-context writes, client-side
// classification and tracking misses) — but a server behavior the state
// machine does not model (see the caveats below) can mis-group statements
// where PostgreSQL's logged identifier could not.
//
// The binary log was considered and rejected as the source: rolled-back
// transactions never reach the binlog, which contradicts the library's core
// semantics (a rolled-back transaction still counts as a unit — a rollback
// proves a partial-write path structurally exists); row-format binlogs lose
// the statement text; and reading the binlog requires the mysqlbinlog tool
// rather than an io.Reader over a text file.
//
// # Required MySQL configuration
//
// The server must write the general query log to a file, with UTC
// timestamps:
//
//	general_log = ON
//	log_output = 'FILE'
//	general_log_file = /path/to/general.log
//	log_timestamps = UTC
//
// (log_timestamps = SYSTEM also parses — the timestamp then carries a
// numeric zone offset — but UTC is the format mycheck is validated against.)
// The TABLE log_output format is not supported. The general query log logs
// every statement of every connection and is meant for test environments:
// its overhead and volume make it unsuitable for production, which is also
// why this whole mechanism targets tests, not monitoring.
//
// # Transaction reconstruction rules
//
// Per thread, in log order, assuming autocommit is enabled (the server
// default):
//
//   - "Query" and "Execute" entries yield statements. "Execute" is the
//     server-side prepared-statement path and is logged with the parameters
//     already substituted into the statement text. "Prepare" entries are
//     ignored (only execution counts, mirroring pgcheck ignoring parse/bind
//     lines); the textual "EXECUTE stmt" Query line that accompanies an
//     SQL-level EXECUTE classifies as neither read nor write, so the
//     statement is not double-counted.
//   - BEGIN / START TRANSACTION opens a transaction. If one is already open
//     on the thread, MySQL implicitly commits it, so the state machine
//     closes it and opens a new one.
//   - COMMIT / ROLLBACK closes the open transaction. A rolled-back
//     transaction keeps its identity — its writes still count as a unit.
//     ROLLBACK TO SAVEPOINT does not close the transaction.
//   - Statements that cause an implicit commit in MySQL close the open
//     transaction, and — being auto-commit statements themselves — form
//     their own unit. mycheck recognizes them by leading keyword: CREATE,
//     ALTER, DROP, RENAME, TRUNCATE, GRANT, REVOKE, INSTALL, UNINSTALL,
//     ANALYZE, CHECK, OPTIMIZE, REPAIR, FLUSH, CACHE, LOCK and UNLOCK, with
//     CREATE TEMPORARY / DROP TEMPORARY excepted (temporary-table DDL does
//     not implicitly commit). The list is deliberately conservative, not
//     exhaustive — MySQL's full implicit-commit set (replication control,
//     RESET, LOAD INDEX INTO CACHE, SET PASSWORD, ...) is larger; an
//     unrecognized implicit-commit statement inside a transaction would
//     leave following statements grouped with it.
//   - "Quit" (connection teardown) while a transaction is open is an
//     implicit rollback: the transaction is closed, and its writes still
//     count as a unit, consistent with the library's rollback semantics.
//   - A SET statement that touches autocommit inside the verified scenario
//     is a hard error, not a guess: with autocommit disabled the grouping
//     rules above are wrong, and silently mis-grouping would produce a
//     false verdict. v1 does not support autocommit-mode changes; run the
//     scenario with autocommit enabled and explicit transactions. Outside
//     the verified slice such statements are ignored.
//
// Statements of different threads interleaved in one log are grouped
// independently per thread and cannot be confused with each other.
//
// The log format understood is the general query log FILE format: a header
// banner plus a "Time Id Command Argument" column line (skipped, also when
// repeated mid-file by a server restart), then one entry per statement —
// ISO-8601 timestamp, a tab, the right-aligned thread id, a space, the
// command word, a tab, and the argument; statements containing newlines
// continue on raw lines that do not start with a timestamp. This format was
// validated empirically against MySQL 8.0, 8.4 (LTS), and 9.7, and matches
// the format MySQL 5.7 documents.
//
// # Correlating log content with a scenario
//
// mycheck cannot know by itself which log entries belong to one scenario;
// the test has to delimit them. Two pragmatic mechanisms, combinable:
//
//   - Record the log file's size before the scenario and parse only the
//     bytes appended after it (the tail), then use Verify.
//   - Execute BeginMarker / EndMarker statements around the scenario —
//     harmless SELECTs whose recognizable literals delimit it in the log —
//     then use VerifyScenario. This is robust against interleaved noise
//     from other connections appearing before or after the scenario, but
//     not against unrelated statements interleaved inside it, so prefer a
//     dedicated database (or at least a quiet one) for such tests.
package mycheck

import (
	"errors"
	"fmt"
	"io"

	"github.com/moznion/go-txnproof"
	"github.com/moznion/go-txnproof/crosscheck"
)

// Checker parses MySQL general query log content and verifies, via
// crosscheck, that the writes in it shared one server-side transaction.
// Create one with New. It implements crosscheck.Parser.
type Checker struct {
	classify txnproof.Classifier
}

var _ crosscheck.Parser = (*Checker)(nil)

// Option configures a Checker.
type Option func(*Checker)

// WithClassifier replaces txnproof.DefaultClassifier for deciding which
// logged statements are writes. The transaction reconstruction also relies
// on the classifier's KindBegin / KindCommit / KindRollback verdicts for
// textual transaction control, so a replacement should keep those intact.
func WithClassifier(c txnproof.Classifier) Option {
	return func(ch *Checker) { ch.classify = c }
}

// New creates a Checker.
func New(opts ...Option) *Checker {
	c := &Checker{classify: txnproof.DefaultClassifier}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Verify parses the given general query log content and checks that all
// writes in it shared one server-side transaction. The returned Report is
// non-nil whenever parsing and checking succeeded, so the caller can inspect
// it even on failure; the error is a *crosscheck.NonAtomicError when the
// writes span two or more transactions.
//
// Feed it only the log content produced by the scenario — typically the file
// tail appended after an offset recorded before the scenario ran (see the
// package documentation). For marker-delimited scenarios use VerifyScenario.
func (c *Checker) Verify(r io.Reader) (*crosscheck.Report, error) {
	stmts, err := c.Parse(r)
	if err != nil {
		return nil, err
	}
	return c.verifyStatements(stmts)
}

// VerifyScenario parses the given general query log content, slices out the
// scenario delimited by BeginMarker / EndMarker statements for the label,
// and checks that all writes in it shared one server-side transaction. The
// returned Report is non-nil whenever parsing, slicing, and checking
// succeeded; the error is a *crosscheck.NonAtomicError when the writes span
// two or more transactions.
func (c *Checker) VerifyScenario(r io.Reader, label string) (*crosscheck.Report, error) {
	stmts, err := c.Parse(r)
	if err != nil {
		return nil, err
	}
	stmts, err = crosscheck.Scenario(stmts, label)
	if err != nil {
		return nil, err
	}
	return c.verifyStatements(stmts)
}

// verifyStatements is the shared tail of Verify and VerifyScenario: it
// rejects autocommit-mode changes inside the verified slice (silently
// mis-grouping would be a false verdict) and delegates the verdict to
// crosscheck.
func (c *Checker) verifyStatements(stmts []crosscheck.Statement) (*crosscheck.Report, error) {
	if err := rejectAutocommitChange(stmts); err != nil {
		return nil, err
	}
	rep, err := crosscheck.VerifyStatements(stmts)
	return rep, adviseOnMissingTxID(err)
}

// rejectAutocommitChange fails when the verified statements contain a SET
// statement that touches autocommit. The transaction reconstruction assumes
// autocommit stays enabled; see the package documentation.
func rejectAutocommitChange(stmts []crosscheck.Statement) error {
	for _, s := range stmts {
		if firstToken(s.SQL) != "SET" || !containsToken(s.SQL, "AUTOCOMMIT") {
			continue
		}
		return fmt.Errorf("mycheck: %sthe scenario changes the autocommit mode (%s); mycheck v1 cannot reconstruct transaction grouping across autocommit-mode changes — run the scenario with autocommit enabled and explicit BEGIN/COMMIT instead",
			lineRef(s), compactSQL(s.SQL))
	}
	return nil
}

// adviseOnMissingTxID wraps a crosscheck.MissingTxIDError with the MySQL
// context. mycheck synthesizes a transaction identity for every statement it
// parses, so this cannot come from Parse output; it fires only when
// statements from another source are checked through this Checker.
func adviseOnMissingTxID(err error) error {
	var me *crosscheck.MissingTxIDError
	if errors.As(err, &me) {
		return fmt.Errorf("%w; mycheck assigns an identity to every statement it parses from a general query log, so this statement did not come from Checker.Parse — verify general-log content with Verify / VerifyScenario instead of building statements by hand", err)
	}
	return err
}

// BeginMarker returns crosscheck.BeginMarker's harmless SELECT to execute
// right before a scenario, re-exported for convenience.
func BeginMarker(label string) string { return crosscheck.BeginMarker(label) }

// EndMarker returns crosscheck.EndMarker's counterpart of BeginMarker, to
// execute right after the scenario, re-exported for convenience.
func EndMarker(label string) string { return crosscheck.EndMarker(label) }

// lineRef renders "line N: " for statements that have a line number.
func lineRef(s crosscheck.Statement) string {
	if s.Line <= 0 {
		return ""
	}
	return fmt.Sprintf("line %d: ", s.Line)
}
