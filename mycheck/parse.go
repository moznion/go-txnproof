package mycheck

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/moznion/go-txnproof"
	"github.com/moznion/go-txnproof/crosscheck"
)

// entryRe matches the first line of one general-log entry: an ISO-8601
// timestamp (a trailing "Z" under log_timestamps = UTC, a numeric offset
// under SYSTEM), a tab, the right-aligned thread id, one space, the command
// word (which can contain spaces, e.g. "Init DB"), a tab, and the first line
// of the argument. Validated against real MySQL 9.7 output, e.g.:
//
//	2026-07-18T01:58:59.266281Z	   11 Query	BEGIN
var entryRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+(?:Z|[+-]\d{2}:\d{2})\t *(\d+) ([^\t]+)\t(.*)$`)

// headerRe matches the column-header line the server writes above the
// entries, also when it reopens the log mid-file (e.g. on restart).
var headerRe = regexp.MustCompile(`^Time\s+Id\s+Command\s+Argument\s*$`)

// isBannerLine reports whether the line belongs to the log-opening banner
// ("<mysqld path>, Version: ... started with:", "Tcp port: ... Unix socket:
// ...", and the column header). Detection is best-effort: a multi-line
// statement whose continuation happens to render exactly like a banner line
// would be truncated at that line.
func isBannerLine(s string) bool {
	if headerRe.MatchString(s) {
		return true
	}
	if strings.HasSuffix(s, "started with:") && strings.Contains(s, ", Version: ") {
		return true
	}
	return strings.HasPrefix(s, "Tcp port: ")
}

// entry is one general-log entry after continuation lines are reassembled.
type entry struct {
	thread  string // the server's thread (connection) id, in decimal
	command string // the command word, e.g. "Query", "Execute", "Quit"
	text    string // the argument (the statement text for Query/Execute)
	line    int    // 1-based line number of the entry's first line
}

// Parse extracts the logged statements from MySQL general query log (FILE
// format) content, implementing crosscheck.Parser. "Query" and "Execute"
// entries yield statements; "Prepare" and the other commands do not.
// Continuation lines of multi-line statements — raw lines that neither start
// a new entry nor belong to a log banner — are appended to the preceding
// entry. Each statement's TxID is the identity synthesized by the per-thread
// transaction reconstruction described in the package documentation
// ("thread <id> tx <n>" inside a transaction, "thread <id> stmt <n>" for
// auto-commit statements). For a string, wrap it with strings.NewReader.
func (c *Checker) Parse(r io.Reader) ([]crosscheck.Statement, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	var entries []entry
	continuing := false // whether the last entry's argument can still grow
	line := 0
	for sc.Scan() {
		line++
		text := sc.Text()
		if m := entryRe.FindStringSubmatch(text); m != nil {
			entries = append(entries, entry{thread: m[1], command: m[2], text: m[3], line: line})
			continuing = true
			continue
		}
		if isBannerLine(text) {
			continuing = false
			continue
		}
		if continuing {
			entries[len(entries)-1].text += "\n" + text
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return c.reconstruct(entries), nil
}

// threadState is the per-thread transaction state machine.
type threadState struct {
	inTx    bool
	txSeq   int // transactions opened on this thread so far
	stmtSeq int // statements seen on this thread so far
}

// reconstruct replays each thread's entry stream through the transaction
// state machine and assigns every statement its synthesized transaction
// identity. See the package documentation for the rules.
func (c *Checker) reconstruct(entries []entry) []crosscheck.Statement {
	states := map[string]*threadState{}
	var stmts []crosscheck.Statement
	for _, e := range entries {
		st := states[e.thread]
		if st == nil {
			st = &threadState{}
			states[e.thread] = st
		}
		switch e.command {
		case "Quit":
			// Connection teardown while a transaction is open is an implicit
			// rollback. Closing the state suffices: the transaction's writes
			// already carry its identity and still count as a unit.
			st.inTx = false
			continue
		case "Query", "Execute":
			// Both carry an executed statement; Execute is the server-side
			// prepared-statement path, logged with parameters substituted.
		default:
			// Connect, Prepare, Init DB, Close stmt, ... carry no executed
			// statement.
			continue
		}
		st.stmtSeq++
		kind := c.classify(e.text)
		var txID string
		switch kind {
		case txnproof.KindBegin:
			// BEGIN / START TRANSACTION while a transaction is already open
			// implicitly commits the open one; opening a new transaction
			// covers both cases.
			st.txSeq++
			st.inTx = true
			txID = txLabel(e.thread, st.txSeq)
		case txnproof.KindCommit, txnproof.KindRollback:
			// A rolled-back transaction keeps its identity: its writes still
			// count as a unit. ROLLBACK TO SAVEPOINT is KindOther and never
			// reaches this case.
			if st.inTx {
				txID = txLabel(e.thread, st.txSeq)
				st.inTx = false
			} else {
				txID = stmtLabel(e.thread, st.stmtSeq)
			}
		default:
			if isImplicitCommit(e.text) {
				// An implicit-commit statement ends the open transaction and
				// is its own auto-commit unit.
				st.inTx = false
				txID = stmtLabel(e.thread, st.stmtSeq)
			} else if st.inTx {
				txID = txLabel(e.thread, st.txSeq)
			} else {
				txID = stmtLabel(e.thread, st.stmtSeq)
			}
		}
		stmts = append(stmts, crosscheck.Statement{SQL: e.text, Kind: kind, TxID: txID, Line: e.line})
	}
	return stmts
}

func txLabel(thread string, seq int) string   { return fmt.Sprintf("thread %s tx %d", thread, seq) }
func stmtLabel(thread string, seq int) string { return fmt.Sprintf("thread %s stmt %d", thread, seq) }

// implicitCommitLeaders are the leading keywords mycheck recognizes as
// implicit-commit statements (DDL, account management, table maintenance,
// locking). Deliberately conservative, not exhaustive; see the package
// documentation.
var implicitCommitLeaders = map[string]struct{}{
	"CREATE": {}, "ALTER": {}, "DROP": {}, "RENAME": {}, "TRUNCATE": {},
	"GRANT": {}, "REVOKE": {}, "INSTALL": {}, "UNINSTALL": {},
	"ANALYZE": {}, "CHECK": {}, "OPTIMIZE": {}, "REPAIR": {}, "FLUSH": {}, "CACHE": {},
	"LOCK": {}, "UNLOCK": {},
}

// isImplicitCommit reports whether the statement implicitly commits an open
// MySQL transaction. Temporary-table DDL (CREATE TEMPORARY / DROP TEMPORARY)
// does not.
func isImplicitCommit(q string) bool {
	first := firstToken(q)
	if _, ok := implicitCommitLeaders[first]; !ok {
		return false
	}
	if (first == "CREATE" || first == "DROP") && secondToken(q) == "TEMPORARY" {
		return false
	}
	return true
}

// The token helpers mirror the root package's unexported ones (classify.go):
// leading whitespace and SQL comments are skipped, tokens are runs of
// identifier characters, comparison is case-insensitive.

// stripLeading removes leading whitespace and SQL comments ("--"/"#" line
// comments and "/* */" block comments; "#" is MySQL-specific).
func stripLeading(q string) string {
	for {
		q = strings.TrimLeft(q, " \t\r\n")
		if strings.HasPrefix(q, "--") || strings.HasPrefix(q, "#") {
			idx := strings.IndexByte(q, '\n')
			if idx < 0 {
				return ""
			}
			q = q[idx+1:]
			continue
		}
		if strings.HasPrefix(q, "/*") {
			idx := strings.Index(q, "*/")
			if idx < 0 {
				return ""
			}
			q = q[idx+2:]
			continue
		}
		return q
	}
}

func firstToken(q string) string {
	q = stripLeading(q)
	end := 0
	for end < len(q) && isIdentChar(q[end]) {
		end++
	}
	return strings.ToUpper(q[:end])
}

func secondToken(q string) string {
	q = stripLeading(q)
	i := 0
	for i < len(q) && isIdentChar(q[i]) {
		i++
	}
	rest := stripLeading(q[i:])
	end := 0
	for end < len(rest) && isIdentChar(rest[end]) {
		end++
	}
	return strings.ToUpper(rest[:end])
}

func isIdentChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

// containsToken reports whether the query contains the given upper-case word
// as a standalone token. "@@autocommit" and "autocommit" both contain the
// token AUTOCOMMIT ('@' is not an identifier character).
func containsToken(q, token string) bool {
	start := -1
	for i := 0; i <= len(q); i++ {
		if i < len(q) && isIdentChar(q[i]) {
			if start < 0 {
				start = i
			}
			continue
		}
		if start >= 0 {
			if strings.ToUpper(q[start:i]) == token {
				return true
			}
			start = -1
		}
	}
	return false
}

// compactSQL collapses whitespace runs so multi-line statements render on
// one message line.
func compactSQL(sql string) string {
	return strings.Join(strings.Fields(sql), " ")
}
