package e2e

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moznion/go-txnproof"
)

// pgxTracer is the reference integration for pgx-native connections (no
// database/sql in the stack): a pgx.QueryTracer that feeds every statement's
// text to a per-connection txnproof.Session. Transaction attribution needs
// nothing extra — pgx's Begin()/Commit()/Rollback() execute textual
// "begin"/"commit"/"rollback", which the Session tracks by itself.
//
// Batches are the one place the text is not enough: a batch is pipelined up
// to a single Sync, so PostgreSQL runs it as one implicit transaction even
// though no BEGIN is ever written. The tracer brackets a batch with
// BeginTx/EndTx — but only when the connection is idle: a batch sent inside
// an explicit transaction already belongs to that transaction, and
// bracketing it would split the outer unit in two.
type pgxTracer struct {
	det *txnproof.Detector

	mu       sync.Mutex
	sessions map[*pgx.Conn]*txnproof.Session
	// implicitBatch marks connections whose current batch the tracer wrapped
	// in a synthesized implicit transaction, so TraceBatchEnd knows whether
	// to close one. Batches do not nest on a connection.
	implicitBatch map[*pgx.Conn]bool
}

var (
	_ pgx.QueryTracer = (*pgxTracer)(nil)
	_ pgx.BatchTracer = (*pgxTracer)(nil)
)

func newPgxTracer(det *txnproof.Detector) *pgxTracer {
	return &pgxTracer{
		det:           det,
		sessions:      make(map[*pgx.Conn]*txnproof.Session),
		implicitBatch: make(map[*pgx.Conn]bool),
	}
}

// configurePool installs the tracer on every connection the pool creates and
// forgets a connection's session when the pool destroys it.
func (t *pgxTracer) configurePool(cfg *pgxpool.Config) {
	cfg.ConnConfig.Tracer = t
	cfg.BeforeClose = t.forget
}

// sessionFor returns the connection's Session, creating it on first use. A
// pgx.Conn executes serially, which is exactly the Session contract; the
// lock only guards the map itself.
func (t *pgxTracer) sessionFor(conn *pgx.Conn) *txnproof.Session {
	t.mu.Lock()
	defer t.mu.Unlock()
	s, ok := t.sessions[conn]
	if !ok {
		s = t.det.NewSession()
		t.sessions[conn] = s
	}
	return s
}

func (t *pgxTracer) forget(conn *pgx.Conn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.sessions, conn)
	delete(t.implicitBatch, conn)
}

func (t *pgxTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	t.sessionFor(conn).Observe(ctx, data.SQL)
	return ctx
}

func (t *pgxTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (t *pgxTracer) TraceBatchStart(ctx context.Context, conn *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	// 'I' = idle: not inside an explicit transaction, so the batch runs as
	// its own implicit one.
	if conn.PgConn().TxStatus() == 'I' {
		t.sessionFor(conn).BeginTx()
		t.mu.Lock()
		t.implicitBatch[conn] = true
		t.mu.Unlock()
	}
	return ctx
}

func (t *pgxTracer) TraceBatchQuery(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
	t.sessionFor(conn).Observe(ctx, data.SQL)
}

func (t *pgxTracer) TraceBatchEnd(_ context.Context, conn *pgx.Conn, _ pgx.TraceBatchEndData) {
	t.mu.Lock()
	implicit := t.implicitBatch[conn]
	delete(t.implicitBatch, conn)
	t.mu.Unlock()
	if implicit {
		t.sessionFor(conn).EndTx()
	}
}
