// Package pgcheck is the PostgreSQL adapter for the crosscheck package: it
// parses the log lines a PostgreSQL server produced while a test scenario
// ran, maps each logged statement to the server-side transaction it ran in,
// and delegates to crosscheck to verify that all write statements shared one
// transaction.
//
// The root txnproof package observes statements on the client side, which has
// known blind spots: statements executed with a detached context, heuristic
// statement classification, and best-effort tracking of textual
// BEGIN/COMMIT. pgcheck complements it with server-side truth for tests that
// run against a real PostgreSQL instance: the server log is the authoritative
// record of which transaction each statement actually ran in.
//
// Everything database-agnostic — grouping writes into units, the verdict and
// its semantics, marker-based scenario slicing (BeginMarker / EndMarker, re-
// exported here for convenience), Report / NonAtomicError — lives in
// crosscheck; this package owns only the PostgreSQL specifics described
// below. See the crosscheck package documentation for the adapter contract
// and the shared semantics.
//
// # Required PostgreSQL configuration
//
// The server must write plain-text (stderr format) logs with English
// message tags, log every statement, and include transaction identifiers in
// the line prefix:
//
//	log_line_prefix = '%m [%p] %q%x %v '
//	log_statement = 'all'
//	lc_messages = 'C'
//
// Meaning of the parts pgcheck relies on:
//
//   - %v is the virtual transaction ID (e.g. "3/42"). It is assigned to
//     every transaction — read-only ones and the implicit transaction of
//     each auto-commit statement included — and log_statement logs each
//     statement right after its transaction acquired one, so it is the
//     reliable grouping key. Two auto-commit writes always show two
//     different virtual transaction IDs; writes inside one transaction
//     share theirs. When no virtual transaction is active PostgreSQL
//     renders a local transaction ID of 0 ("0/0", or "N/0" on older
//     versions), which pgcheck treats as no identity.
//   - %x is the real transaction ID, or 0 if none is assigned. It is the
//     fallback grouping key for prefixes without %v, but it is weaker:
//     reads never get one, and a transaction's early log lines show 0
//     until a statement actually forced an ID to be assigned. In
//     particular, do not rely on log_min_duration_statement timing to see
//     %x: the duration line of an auto-commit statement is emitted after
//     its implicit transaction already ended, with %x back to 0 (and %v
//     rendered as 0/0) — observed on PostgreSQL 18.
//   - %q makes the parts after it disappear from non-session (background
//     process) lines, keeping them from confusing the parser.
//   - lc_messages must produce English log tags ("LOG:", "statement:",
//     "duration:", "execute"); the parser does not understand localized
//     ones.
//
// Each statement's crosscheck transaction identity is the virtual
// transaction ID when one was active, falling back to the real transaction
// ID (rendered as "xid <n>"). A different log_line_prefix can be used via
// WithLogLinePrefix (translated automatically) or WithPrefixPattern (a
// hand-written regular expression); it must include %x and/or %v, and
// including %v is strongly recommended.
//
// Both the simple query protocol ("statement: ..." lines) and the extended
// query protocol ("execute ...: ..." lines, as produced by pgx) are
// understood; "parse"/"bind" phase lines are ignored so statements are not
// double-counted. Multi-line statements (continuation lines, which the
// stderr format indents with a tab and no prefix) are reassembled. The CSV
// and JSON log formats are not supported.
//
// # Correlating log lines with a scenario
//
// pgcheck cannot know by itself which log lines belong to one scenario; the
// test has to delimit them. Two pragmatic mechanisms, combinable:
//
//   - Record the log file's size before the scenario and parse only the
//     bytes appended after it (the tail), then use Verify.
//   - Execute BeginMarker / EndMarker statements around the scenario —
//     harmless SELECTs whose recognizable literals delimit it in the log —
//     then use VerifyScenario. This is robust against interleaved noise
//     from other connections appearing before or after the scenario, but
//     not against unrelated statements interleaved inside it, so prefer a
//     dedicated database (or at least a quiet one) for such tests.
package pgcheck

import (
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	txnproof "github.com/moznion/go-txnproof"
	"github.com/moznion/go-txnproof/crosscheck"
)

// DefaultLogLinePrefix is the log_line_prefix value pgcheck assumes when
// WithLogLinePrefix / WithPrefixPattern is not given. Set it verbatim in
// postgresql.conf:
//
//	log_line_prefix = '%m [%p] %q%x %v '
const DefaultLogLinePrefix = "%m [%p] %q%x %v "

// Checker parses PostgreSQL server log content and verifies, via crosscheck,
// that the writes in it shared one server-side transaction. Create one with
// New. It implements crosscheck.Parser.
type Checker struct {
	prefixRe *regexp.Regexp
	classify txnproof.Classifier
}

var _ crosscheck.Parser = (*Checker)(nil)

// Option configures a Checker.
type Option func(*settings)

type settings struct {
	prefix   string
	pattern  *regexp.Regexp
	classify txnproof.Classifier
}

// WithLogLinePrefix sets the postgresql.conf log_line_prefix value the log
// was produced with (e.g. "%m [%p] %q%x %v "). It is translated into a line
// prefix pattern via CompileLogLinePrefix; the prefix must contain %x and/or
// %v (see the package documentation for why %v is strongly recommended).
func WithLogLinePrefix(prefix string) Option {
	return func(s *settings) { s.prefix = prefix }
}

// WithPrefixPattern sets a hand-written regular expression matching the line
// prefix, for prefixes CompileLogLinePrefix cannot translate. It is matched
// at the start of each line and must capture the transaction identifiers as
// named groups "xid" (%x) and/or "vxid" (%v); everything after the match is
// interpreted as the log message body. Takes precedence over
// WithLogLinePrefix.
func WithPrefixPattern(re *regexp.Regexp) Option {
	return func(s *settings) { s.pattern = re }
}

// WithClassifier replaces txnproof.DefaultClassifier for deciding which
// logged statements are writes.
func WithClassifier(c txnproof.Classifier) Option {
	return func(s *settings) { s.classify = c }
}

// New creates a Checker. It fails if the configured log_line_prefix cannot
// be translated or the prefix pattern captures neither "xid" nor "vxid".
func New(opts ...Option) (*Checker, error) {
	s := &settings{
		prefix:   DefaultLogLinePrefix,
		classify: txnproof.DefaultClassifier,
	}
	for _, o := range opts {
		o(s)
	}
	re := s.pattern
	if re == nil {
		var err error
		re, err = CompileLogLinePrefix(s.prefix)
		if err != nil {
			return nil, err
		}
	}
	if !hasGroup(re, "xid") && !hasGroup(re, "vxid") {
		return nil, fmt.Errorf("pgcheck: prefix pattern %q captures neither a named group \"xid\" (%%x) nor \"vxid\" (%%v); transaction identifiers cannot be extracted", re)
	}
	return &Checker{prefixRe: re, classify: s.classify}, nil
}

// Verify parses the given log content and checks that all writes in it
// shared one server-side transaction. The returned Report is non-nil
// whenever parsing and checking succeeded, so the caller can inspect it even
// on failure; the error is a *crosscheck.NonAtomicError when the writes span
// two or more transactions.
//
// Feed it only the log content produced by the scenario — typically the file
// tail appended after an offset recorded before the scenario ran (see the
// package documentation). For marker-delimited scenarios use VerifyScenario.
func (c *Checker) Verify(r io.Reader) (*crosscheck.Report, error) {
	rep, err := crosscheck.Verify(c, r)
	return rep, adviseOnMissingTxID(err)
}

// VerifyScenario parses the given log content, slices out the scenario
// delimited by BeginMarker / EndMarker statements for the label, and checks
// that all writes in it shared one server-side transaction. The returned
// Report is non-nil whenever parsing, slicing, and checking succeeded; the
// error is a *crosscheck.NonAtomicError when the writes span two or more
// transactions.
func (c *Checker) VerifyScenario(r io.Reader, label string) (*crosscheck.Report, error) {
	rep, err := crosscheck.VerifyScenario(c, r, label)
	return rep, adviseOnMissingTxID(err)
}

// adviseOnMissingTxID wraps a crosscheck.MissingTxIDError with the
// PostgreSQL-specific way out: a write with neither an active virtual txid
// nor an assigned txid usually means log_line_prefix lacks %v, or the line
// was logged at a moment when both identifiers were unavailable (see the
// package documentation).
func adviseOnMissingTxID(err error) error {
	var me *crosscheck.MissingTxIDError
	if errors.As(err, &me) {
		return fmt.Errorf("%w; include %%v in log_line_prefix and log statements with log_statement = 'all' so every line carries a virtual transaction ID", err)
	}
	return err
}

// BeginMarker returns crosscheck.BeginMarker's harmless SELECT to execute
// right before a scenario, re-exported for convenience.
func BeginMarker(label string) string { return crosscheck.BeginMarker(label) }

// EndMarker returns crosscheck.EndMarker's counterpart of BeginMarker, to
// execute right after the scenario, re-exported for convenience.
func EndMarker(label string) string { return crosscheck.EndMarker(label) }

// CompileLogLinePrefix translates a postgresql.conf log_line_prefix value
// into a regular expression that matches the rendered prefix at the start of
// a session log line, capturing %x as the named group "xid" and %v as
// "vxid". Free-form escapes (%a, %u, %d, %r, %h, %b, %i) are matched
// lazily, so avoid prefixes where such a field is directly adjacent to %x or
// %v without a distinctive literal in between.
func CompileLogLinePrefix(prefix string) (*regexp.Regexp, error) {
	var sb strings.Builder
	sb.WriteString("^")
	for i := 0; i < len(prefix); i++ {
		c := prefix[i]
		if c != '%' {
			sb.WriteString(regexp.QuoteMeta(string(c)))
			continue
		}
		i++
		if i >= len(prefix) {
			return nil, fmt.Errorf("pgcheck: log_line_prefix %q ends with a bare %%", prefix)
		}
		switch prefix[i] {
		case 'x':
			sb.WriteString(`(?P<xid>\d+)`)
		case 'v':
			// Empty when no virtual transaction ID is available.
			sb.WriteString(`(?P<vxid>\d+/\d+)?`)
		case 'p', 'P', 'l':
			sb.WriteString(`\d+`)
		case 'm':
			sb.WriteString(`\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ \S+`)
		case 't', 's':
			sb.WriteString(`\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \S+`)
		case 'n':
			sb.WriteString(`\d+\.\d+`)
		case 'c':
			sb.WriteString(`[0-9a-f]+\.[0-9a-f]+`)
		case 'e':
			sb.WriteString(`[0-9A-Z]{5}`)
		case 'Q':
			sb.WriteString(`-?\d+`)
		case 'q':
			// Session processes ignore %q, so on the statement lines this
			// parser cares about it renders nothing.
		case '%':
			sb.WriteString("%")
		case 'a', 'u', 'd', 'r', 'h', 'b', 'i':
			// Free-form fields (application name, user, database,
			// remote host, backend type, command tag); match lazily.
			sb.WriteString(`.*?`)
		default:
			return nil, fmt.Errorf("pgcheck: unsupported log_line_prefix escape %%%c; use WithPrefixPattern with a hand-written pattern instead", prefix[i])
		}
	}
	return regexp.Compile(sb.String())
}

func hasGroup(re *regexp.Regexp, name string) bool {
	for _, n := range re.SubexpNames() {
		if n == name {
			return true
		}
	}
	return false
}
