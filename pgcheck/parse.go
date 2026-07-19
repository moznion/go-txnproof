package pgcheck

import (
	"bufio"
	"io"
	"regexp"
	"strings"

	"github.com/moznion/go-txnproof/crosscheck"
)

// bodyRe matches the message body (after the line prefix) of the log lines
// that carry a statement text: "statement: ..." lines from log_statement,
// "duration: ... statement: ..." lines from log_min_duration_statement, and
// their extended-query-protocol counterparts "execute <name>: ...".
// "parse"/"bind" phase lines and non-LOG lines (ERROR, DETAIL, STATEMENT,
// ...) deliberately do not match; counting them would duplicate statements.
var bodyRe = regexp.MustCompile(`^LOG:\s+(?:duration: [0-9.]+ ms\s+)?(?:statement:|execute [^:]*:)\s?(.*)$`)

// Parse extracts the logged statements from plain-text (stderr format)
// PostgreSQL log content, implementing crosscheck.Parser. Lines that do not
// carry a statement are skipped; tab-indented continuation lines are
// appended to the preceding statement. Each statement's TxID is the virtual
// transaction ID (%v) when one was active (a "0/0" or "N/0" rendering means
// none was), falling back to the real transaction ID (%x, rendered as
// "xid <n>") — or empty when the server logged neither, which is normal for
// reads. For a string, wrap it with strings.NewReader.
func (c *Checker) Parse(r io.Reader) ([]crosscheck.Statement, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	var stmts []crosscheck.Statement
	continuing := false // whether the last parsed statement can still grow
	line := 0
	for sc.Scan() {
		line++
		text := sc.Text()
		if strings.HasPrefix(text, "\t") {
			// Continuation line of a multi-line log message: no prefix,
			// one leading tab.
			if continuing {
				stmts[len(stmts)-1].SQL += "\n" + strings.TrimPrefix(text, "\t")
			}
			continue
		}
		continuing = false
		m := c.prefixRe.FindStringSubmatchIndex(text)
		if m == nil {
			continue
		}
		body := bodyRe.FindStringSubmatch(text[m[1]:])
		if body == nil {
			continue
		}
		st := crosscheck.Statement{SQL: body[1], Line: line}
		var xid string
		for i, name := range c.prefixRe.SubexpNames() {
			if i == 0 || m[2*i] < 0 {
				continue
			}
			val := text[m[2*i]:m[2*i+1]]
			switch name {
			case "xid":
				xid = val
			case "vxid":
				// A local transaction ID of 0 ("0/0", or "N/0" on older
				// PostgreSQL versions) means no virtual transaction was
				// active when the line was logged — it is not an identity,
				// and treating it as one would group unrelated statements
				// into a single fake transaction.
				if !strings.HasSuffix(val, "/0") {
					st.TxID = val
				}
			}
		}
		if st.TxID == "" && xid != "" && xid != "0" {
			// No virtual transaction ID in the prefix; fall back to the
			// real transaction ID. 0 means none was assigned, which must
			// stay an empty identity.
			st.TxID = "xid " + xid
		}
		stmts = append(stmts, st)
		continuing = true
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	// Classify once the continuation lines are complete.
	for i := range stmts {
		stmts[i].Kind = c.classify(stmts[i].SQL)
	}
	return stmts, nil
}
