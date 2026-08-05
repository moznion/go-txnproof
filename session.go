package txnproof

import "context"

// Session is the observation surface for one database connection that does
// not go through the database/sql driver stack: a native driver (pgx, a
// ClickHouse native client, ...) or an ORM hook that exposes statement text.
// The driver middleware itself is built on it, so both paths share one
// transaction-attribution state machine and one set of counting rules.
//
// Create exactly one Session per underlying connection and use it the way
// the connection itself must be used: serially. That mirrors the guarantee
// database/sql gives a driver.Conn; a Session has no locking of its own.
// Statements from different connections must go to different Sessions —
// transaction attribution is per connection, and mixing connections in one
// Session would merge unrelated transactions into one unit.
//
// Observe every statement the connection executes, at most once, with the
// context that carried it (that context is what attributes the statement to
// a boundary). Transaction control that the driver executes as statement
// text ("BEGIN"/"COMMIT"/"ROLLBACK" — pgx's Begin()/Commit() do exactly
// that) is tracked from the text alone; BeginTx/EndTx exist for transaction
// transitions that never surface as text.
//
// Like the driver middleware, a Session observes submitted statements
// whether or not they later succeed: a failed write still proves a partial-
// write path structurally exists in the boundary. The one exception the
// middleware makes — ErrBadConn, where database/sql re-runs the statement on
// a fresh connection — is the integration's to make: skip observing a
// statement only when something else will observe its retry.
type Session struct {
	det *Detector
	// txID is non-zero while the connection is inside a transaction — the
	// same field a wrappedConn tracks, shared here so every observation
	// path attributes statements identically.
	txID uint64
}

// NewSession creates a Session bound to the Detector. One Session per
// connection; see the type comment for the contract.
func (d *Detector) NewSession() *Session { return &Session{det: d} }

// Observe records one executed statement, updating textual transaction
// state ("BEGIN"/"COMMIT"/"ROLLBACK" executed as statements) exactly like
// the driver middleware does.
func (s *Session) Observe(ctx context.Context, query string) {
	s.observeKind(ctx, query, s.det.classify(query))
}

// observeKind is Observe with the classification already done: the
// prepared-statement path classifies once at Prepare and reuses the result
// for every execution.
func (s *Session) observeKind(ctx context.Context, query string, kind StatementKind) {
	switch kind {
	case KindBegin:
		if s.txID == 0 {
			s.txID = s.det.nextTxID()
		}
	case KindCommit, KindRollback:
		s.txID = 0
	}
	s.det.record(ctx, query, kind, s.txID)
}

// BeginTx marks a transaction start that does not surface as statement
// text. Two callers need it: driver-API-level transactions (database/sql's
// ConnBeginTx, mirrored by the driver middleware), and protocol-level
// implicit transactions — a pgx batch is pipelined up to a single Sync, so
// PostgreSQL runs it as one implicit transaction even though no BEGIN is
// ever written; the integration brackets the batch with BeginTx/EndTx to
// count it as the single unit it is. Statements observed before the
// matching EndTx are attributed to this transaction.
//
// Unlike a textual "BEGIN" (which is a no-op inside a transaction, matching
// the server's behavior), BeginTx trusts the caller and starts a new unit
// unconditionally — do not call it when the connection may already be in a
// transaction (a batch sent inside an explicit transaction belongs to that
// transaction; bracketing it would split the outer unit in two).
func (s *Session) BeginTx() { s.txID = s.det.nextTxID() }

// EndTx marks the end of a transaction started by BeginTx — commit and
// rollback alike, since a rolled-back transaction still counts as a unit.
func (s *Session) EndTx() { s.txID = 0 }
