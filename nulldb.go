package txnproof

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
)

// NewNullDB returns a *sql.DB backed by an in-memory no-op driver wrapped by
// the Detector. Every statement succeeds and returns no rows; only the
// statement/transaction timeline is observed.
//
// This is the sqlmock-free way to unit-test atomicity: inject the returned DB
// where your code expects a *sql.DB, run the use case inside a boundary, and
// assert no violations were reported. Unlike sqlmock, no expectations need to
// be declared.
func (d *Detector) NewNullDB() *sql.DB {
	return sql.OpenDB(d.WrapConnector(nullConnector{}))
}

type nullConnector struct{}

func (nullConnector) Connect(context.Context) (driver.Conn, error) { return nullConn{}, nil }
func (nullConnector) Driver() driver.Driver                        { return nullDriver{} }

type nullDriver struct{}

func (nullDriver) Open(string) (driver.Conn, error) { return nullConn{}, nil }

type nullConn struct{}

var (
	_ driver.Conn           = nullConn{}
	_ driver.ExecerContext  = nullConn{}
	_ driver.QueryerContext = nullConn{}
)

func (nullConn) Prepare(query string) (driver.Stmt, error) { return nullStmt{}, nil }
func (nullConn) Close() error                              { return nil }
func (nullConn) Begin() (driver.Tx, error)                 { return nullTx{}, nil }

func (nullConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}

func (nullConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &nullRows{}, nil
}

type nullTx struct{}

func (nullTx) Commit() error   { return nil }
func (nullTx) Rollback() error { return nil }

type nullStmt struct{}

func (nullStmt) Close() error  { return nil }
func (nullStmt) NumInput() int { return -1 }

func (nullStmt) Exec([]driver.Value) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}

func (nullStmt) Query([]driver.Value) (driver.Rows, error) {
	return &nullRows{}, nil
}

type nullRows struct{}

func (*nullRows) Columns() []string              { return nil }
func (*nullRows) Close() error                   { return nil }
func (*nullRows) Next(dest []driver.Value) error { return io.EOF }
