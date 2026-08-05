package txnproof

import (
	"context"
	"database/sql/driver"
	"errors"
)

// Wrap wraps a database/sql driver so that every statement executed through it
// is observed by the Detector. Register the result with sql.Register:
//
//	sql.Register("pgx-txnproof", detector.Wrap(stdlib.GetDefaultDriver()))
//	db, err := sql.Open("pgx-txnproof", dsn)
func (d *Detector) Wrap(drv driver.Driver) driver.Driver {
	return &wrappedDriver{det: d, drv: drv}
}

// WrapConnector wraps a driver.Connector for use with sql.OpenDB.
func (d *Detector) WrapConnector(c driver.Connector) driver.Connector {
	return &wrappedConnector{det: d, connector: c}
}

type wrappedDriver struct {
	det *Detector
	drv driver.Driver
}

func (w *wrappedDriver) Open(name string) (driver.Conn, error) {
	conn, err := w.drv.Open(name)
	if err != nil {
		return nil, err
	}
	return &wrappedConn{det: w.det, conn: conn, session: Session{det: w.det}}, nil
}

type wrappedConnector struct {
	det       *Detector
	connector driver.Connector
}

func (w *wrappedConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := w.connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &wrappedConn{det: w.det, conn: conn, session: Session{det: w.det}}, nil
}

func (w *wrappedConnector) Driver() driver.Driver {
	return &wrappedDriver{det: w.det, drv: w.connector.Driver()}
}

// wrappedConn observes every statement and tracks whether the connection is
// currently inside a transaction. The tracking itself lives in Session — the
// same state machine serves native-driver integrations — and database/sql
// guarantees a driver.Conn is used by a single goroutine at a time, which is
// exactly the serial-use contract Session requires.
type wrappedConn struct {
	det     *Detector
	conn    driver.Conn
	session Session
}

var (
	_ driver.Conn               = (*wrappedConn)(nil)
	_ driver.ConnPrepareContext = (*wrappedConn)(nil)
	_ driver.ConnBeginTx        = (*wrappedConn)(nil)
	_ driver.ExecerContext      = (*wrappedConn)(nil)
	_ driver.QueryerContext     = (*wrappedConn)(nil)
	_ driver.Pinger             = (*wrappedConn)(nil)
	_ driver.SessionResetter    = (*wrappedConn)(nil)
	_ driver.Validator          = (*wrappedConn)(nil)
	_ driver.NamedValueChecker  = (*wrappedConn)(nil)
)

// observe records one executed statement, updating textual transaction state
// (raw "BEGIN"/"COMMIT" executed as statements) as a best effort.
func (c *wrappedConn) observe(ctx context.Context, query string) {
	c.session.Observe(ctx, query)
}

func (c *wrappedConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &wrappedStmt{conn: c, stmt: stmt, query: query, kind: c.det.classify(query)}, nil
}

func (c *wrappedConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if pc, ok := c.conn.(driver.ConnPrepareContext); ok {
		stmt, err := pc.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		return &wrappedStmt{conn: c, stmt: stmt, query: query, kind: c.det.classify(query)}, nil
	}
	return c.Prepare(query)
}

func (c *wrappedConn) Close() error { return c.conn.Close() }

func (c *wrappedConn) Begin() (driver.Tx, error) {
	tx, err := c.conn.Begin() //nolint:staticcheck // legacy interface must be supported
	if err != nil {
		return nil, err
	}
	c.session.BeginTx()
	return &wrappedTx{conn: c, tx: tx}, nil
}

func (c *wrappedConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if bt, ok := c.conn.(driver.ConnBeginTx); ok {
		tx, err := bt.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
		c.session.BeginTx()
		return &wrappedTx{conn: c, tx: tx}, nil
	}
	if opts != (driver.TxOptions{}) {
		return nil, errors.New("txnproof: underlying driver does not support non-default transaction options")
	}
	return c.Begin()
}

func (c *wrappedConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	res, err := c.execUnderlying(ctx, query, args)
	if errors.Is(err, driver.ErrSkip) {
		// database/sql falls back to the prepared-statement path, which is
		// also wrapped; recording here would double-count.
		return nil, driver.ErrSkip
	}
	if errors.Is(err, driver.ErrBadConn) {
		// database/sql retries on a fresh connection; the retry will be
		// recorded, so recording here would double-count.
		return nil, err
	}
	c.observe(ctx, query)
	return res, err
}

func (c *wrappedConn) execUnderlying(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if ec, ok := c.conn.(driver.ExecerContext); ok {
		return ec.ExecContext(ctx, query, args)
	}
	if e, ok := c.conn.(driver.Execer); ok { //nolint:staticcheck // legacy interface fallback
		vals, err := namedValuesToValues(args)
		if err != nil {
			return nil, err
		}
		return e.Exec(query, vals)
	}
	return nil, driver.ErrSkip
}

func (c *wrappedConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.queryUnderlying(ctx, query, args)
	if errors.Is(err, driver.ErrSkip) {
		return nil, driver.ErrSkip
	}
	if errors.Is(err, driver.ErrBadConn) {
		return nil, err
	}
	c.observe(ctx, query)
	return rows, err
}

func (c *wrappedConn) queryUnderlying(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if qc, ok := c.conn.(driver.QueryerContext); ok {
		return qc.QueryContext(ctx, query, args)
	}
	if q, ok := c.conn.(driver.Queryer); ok { //nolint:staticcheck // legacy interface fallback
		vals, err := namedValuesToValues(args)
		if err != nil {
			return nil, err
		}
		return q.Query(query, vals)
	}
	return nil, driver.ErrSkip
}

func (c *wrappedConn) Ping(ctx context.Context) error {
	if p, ok := c.conn.(driver.Pinger); ok {
		return p.Ping(ctx)
	}
	return nil
}

func (c *wrappedConn) ResetSession(ctx context.Context) error {
	if sr, ok := c.conn.(driver.SessionResetter); ok {
		return sr.ResetSession(ctx)
	}
	return nil
}

func (c *wrappedConn) IsValid() bool {
	if v, ok := c.conn.(driver.Validator); ok {
		return v.IsValid()
	}
	return true
}

func (c *wrappedConn) CheckNamedValue(nv *driver.NamedValue) error {
	if nvc, ok := c.conn.(driver.NamedValueChecker); ok {
		return nvc.CheckNamedValue(nv)
	}
	return driver.ErrSkip
}

type wrappedTx struct {
	conn *wrappedConn
	tx   driver.Tx
}

func (t *wrappedTx) Commit() error {
	err := t.tx.Commit()
	t.conn.session.EndTx()
	return err
}

func (t *wrappedTx) Rollback() error {
	err := t.tx.Rollback()
	t.conn.session.EndTx()
	return err
}

type wrappedStmt struct {
	conn  *wrappedConn
	stmt  driver.Stmt
	query string
	// kind is classified once at Prepare and reused for every execution, so
	// a reused prepared statement does not re-pay classification (a
	// data-modifying CTE costs a full-text token scan). The classifier is
	// fixed after Detector construction and driver.Stmt is used by a single
	// goroutine, so no locking is needed.
	kind StatementKind
}

var (
	_ driver.Stmt              = (*wrappedStmt)(nil)
	_ driver.StmtExecContext   = (*wrappedStmt)(nil)
	_ driver.StmtQueryContext  = (*wrappedStmt)(nil)
	_ driver.NamedValueChecker = (*wrappedStmt)(nil)
)

func (s *wrappedStmt) Close() error  { return s.stmt.Close() }
func (s *wrappedStmt) NumInput() int { return s.stmt.NumInput() }

func (s *wrappedStmt) Exec(args []driver.Value) (driver.Result, error) {
	res, err := s.stmt.Exec(args) //nolint:staticcheck // legacy interface must be supported
	if errors.Is(err, driver.ErrBadConn) {
		return nil, err
	}
	s.conn.session.observeKind(context.Background(), s.query, s.kind)
	return res, err
}

func (s *wrappedStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	res, err := s.execUnderlying(ctx, args)
	if errors.Is(err, driver.ErrBadConn) {
		return nil, err
	}
	s.conn.session.observeKind(ctx, s.query, s.kind)
	return res, err
}

func (s *wrappedStmt) execUnderlying(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if sec, ok := s.stmt.(driver.StmtExecContext); ok {
		return sec.ExecContext(ctx, args)
	}
	vals, err := namedValuesToValues(args)
	if err != nil {
		return nil, err
	}
	return s.stmt.Exec(vals) //nolint:staticcheck // legacy interface fallback
}

func (s *wrappedStmt) Query(args []driver.Value) (driver.Rows, error) {
	rows, err := s.stmt.Query(args) //nolint:staticcheck // legacy interface must be supported
	if errors.Is(err, driver.ErrBadConn) {
		return nil, err
	}
	s.conn.session.observeKind(context.Background(), s.query, s.kind)
	return rows, err
}

func (s *wrappedStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := s.queryUnderlying(ctx, args)
	if errors.Is(err, driver.ErrBadConn) {
		return nil, err
	}
	s.conn.session.observeKind(ctx, s.query, s.kind)
	return rows, err
}

func (s *wrappedStmt) queryUnderlying(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if sqc, ok := s.stmt.(driver.StmtQueryContext); ok {
		return sqc.QueryContext(ctx, args)
	}
	vals, err := namedValuesToValues(args)
	if err != nil {
		return nil, err
	}
	return s.stmt.Query(vals) //nolint:staticcheck // legacy interface fallback
}

func (s *wrappedStmt) CheckNamedValue(nv *driver.NamedValue) error {
	if nvc, ok := s.stmt.(driver.NamedValueChecker); ok {
		return nvc.CheckNamedValue(nv)
	}
	if nvc, ok := s.conn.conn.(driver.NamedValueChecker); ok {
		return nvc.CheckNamedValue(nv)
	}
	return driver.ErrSkip
}

func namedValuesToValues(named []driver.NamedValue) ([]driver.Value, error) {
	vals := make([]driver.Value, len(named))
	for i, nv := range named {
		if nv.Name != "" {
			return nil, errors.New("txnproof: named parameters are not supported by the underlying driver")
		}
		vals[i] = nv.Value
	}
	return vals, nil
}
