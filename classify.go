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
	switch firstToken(query) {
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
		if secondToken(query) == "TO" {
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

var writeTokens = map[string]struct{}{
	"INSERT": {}, "UPDATE": {}, "DELETE": {}, "MERGE": {}, "TRUNCATE": {}, "REPLACE": {},
}

// containsWriteToken reports whether the query contains a standalone write
// keyword anywhere. Used for WITH-prefixed statements (data-modifying CTEs).
func containsWriteToken(q string) bool {
	start := -1
	for i := 0; i <= len(q); i++ {
		if i < len(q) && isIdentChar(q[i]) {
			if start < 0 {
				start = i
			}
			continue
		}
		if start >= 0 {
			if _, ok := writeTokens[strings.ToUpper(q[start:i])]; ok {
				return true
			}
			start = -1
		}
	}
	return false
}
