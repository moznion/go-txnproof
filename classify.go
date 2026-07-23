package txnproof

import "strings"

// StatementKind is the coarse classification of a SQL statement that txnproof
// cares about for atomicity tracking.
type StatementKind int

const (
	// KindOther is a statement that is neither a read, a write, nor a
	// transaction-control statement (e.g. SET, SAVEPOINT, LOCK).
	KindOther StatementKind = iota
	// KindRead is a statement that does not modify data.
	KindRead
	// KindWrite is a statement that modifies data.
	KindWrite
	// KindBegin starts a transaction (textual BEGIN / START TRANSACTION).
	KindBegin
	// KindCommit commits a transaction (textual COMMIT / END).
	KindCommit
	// KindRollback rolls back a transaction (textual ROLLBACK / ABORT).
	KindRollback
)

func (k StatementKind) String() string {
	switch k {
	case KindRead:
		return "read"
	case KindWrite:
		return "write"
	case KindBegin:
		return "begin"
	case KindCommit:
		return "commit"
	case KindRollback:
		return "rollback"
	default:
		return "other"
	}
}

// Classifier decides the StatementKind of a raw SQL string.
type Classifier func(query string) StatementKind

// DefaultClassifier classifies a statement by its leading keyword, skipping
// leading whitespace and SQL comments. It is a heuristic:
//
//   - DML (INSERT/UPDATE/DELETE/MERGE/...), DDL (CREATE/ALTER/DROP/...), and
//     procedure calls (CALL/DO) are treated as writes. Procedure calls are
//     classified conservatively because their body is opaque.
//   - WITH-prefixed statements are scanned for embedded write keywords so that
//     data-modifying CTEs (WITH ... INSERT/UPDATE/DELETE) count as writes. The
//     scan is token-based and may misfire on write keywords inside string
//     literals; override with WithClassifier if this matters for your queries.
//   - EXPLAIN is treated as a read even though EXPLAIN ANALYZE executes the
//     inner statement.
func DefaultClassifier(query string) StatementKind {
	// Uppercase the leading token into a stack buffer and switch on it. Keeping
	// the string(buf[:n]) conversion inside the switch lets the compiler prove
	// it does not escape, so classification stays allocation-free regardless of
	// the query's original letter case.
	var buf [maxKeywordLen]byte
	n, ok := asciiUpperToken(leadingToken(query), buf[:])
	if !ok {
		// Token longer than any recognized keyword: cannot match.
		return KindOther
	}
	switch string(buf[:n]) {
	case "SELECT", "SHOW", "EXPLAIN", "VALUES", "TABLE", "FETCH", "DECLARE":
		return KindRead
	case "INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE", "REPLACE", "UPSERT", "COPY", "IMPORT",
		"CREATE", "ALTER", "DROP", "GRANT", "REVOKE", "COMMENT", "REFRESH",
		"CALL", "DO":
		return KindWrite
	case "BEGIN", "START":
		return KindBegin
	case "COMMIT", "END":
		return KindCommit
	case "ROLLBACK", "ABORT":
		// ROLLBACK TO [SAVEPOINT] does not end the transaction.
		if secondTokenIsTo(query) {
			return KindOther
		}
		return KindRollback
	case "WITH":
		if containsWriteToken(query) {
			return KindWrite
		}
		return KindRead
	default:
		return KindOther
	}
}

// stripLeading removes leading whitespace and SQL comments ("--" line comments
// and "/* */" block comments).
func stripLeading(q string) string {
	for {
		q = strings.TrimLeft(q, " \t\r\n")
		if strings.HasPrefix(q, "--") {
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

// maxKeywordLen is the length of the longest keyword DefaultClassifier and the
// data-modifying-CTE scan recognize (TRUNCATE / ROLLBACK). A token longer than
// this cannot match any keyword, so it never needs to be buffered.
const maxKeywordLen = 8

// leadingToken returns the leading identifier token of q (after stripping
// whitespace and comments) as a sub-slice of q, without allocating or changing
// case.
func leadingToken(q string) string {
	q = stripLeading(q)
	end := 0
	for end < len(q) && isIdentChar(q[end]) {
		end++
	}
	return q[:end]
}

// asciiUpperToken copies tok into buf, ASCII-uppercasing as it goes, and
// returns the byte count written. ok is false when tok does not fit in buf,
// which means tok is longer than any recognized keyword and cannot match one.
// Uppercasing into a caller-provided (stack) buffer avoids the allocation that
// strings.ToUpper incurs whenever the token contains a lowercase letter.
func asciiUpperToken(tok string, buf []byte) (n int, ok bool) {
	if len(tok) > len(buf) {
		return 0, false
	}
	for i := 0; i < len(tok); i++ {
		c := tok[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		buf[i] = c
	}
	return len(tok), true
}

// secondTokenIsTo reports whether the token following the leading token is TO
// (case-insensitive), i.e. a ROLLBACK TO [SAVEPOINT] that must not end the tx.
func secondTokenIsTo(q string) bool {
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
	tok := rest[:end]
	return len(tok) == 2 &&
		(tok[0] == 'T' || tok[0] == 't') &&
		(tok[1] == 'O' || tok[1] == 'o')
}

func isIdentChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

// containsWriteToken reports whether the query contains a standalone write
// keyword anywhere. Used for WITH-prefixed statements (data-modifying CTEs).
// The per-token uppercasing goes through a stack buffer so the scan allocates
// nothing regardless of the query's letter case.
func containsWriteToken(q string) bool {
	var buf [maxKeywordLen]byte
	start := -1
	for i := 0; i <= len(q); i++ {
		if i < len(q) && isIdentChar(q[i]) {
			if start < 0 {
				start = i
			}
			continue
		}
		if start >= 0 {
			if n, ok := asciiUpperToken(q[start:i], buf[:]); ok {
				switch string(buf[:n]) {
				case "INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE", "REPLACE":
					return true
				}
			}
			start = -1
		}
	}
	return false
}
