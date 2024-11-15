package gorm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"log"
)

// MySQLDriverWrapper wraps the original MySQL driver
type MySQLDriverWrapper struct {
	originalDriver *mysql.MySQLDriver
}

// Open wraps the Open method of the original MySQL driver
func (d *MySQLDriverWrapper) Open(name string) (driver.Conn, error) {
	conn, err := d.originalDriver.Open(name)
	if err != nil {
		return nil, err
	}
	return &MySQLConnWrapper{conn: conn}, nil
}

// MySQLConnWrapper wraps the original MySQL connection
type MySQLConnWrapper struct {
	conn driver.Conn
}

// Prepare wraps the Prepare method of the original MySQL connection
func (c *MySQLConnWrapper) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &MySQLStmtWrapper{stmt: stmt}, nil
}

// Close wraps the Close method of the original MySQL connection
func (c *MySQLConnWrapper) Close() error {
	return c.conn.Close()
}

// Begin wraps the Begin method of the original MySQL connection
func (c *MySQLConnWrapper) Begin() (driver.Tx, error) {
	log.Printf("Beginning transaction")
	tx, err := c.conn.Begin()
	if err != nil {
		return nil, err
	}
	return &MySQLTxWrapper{tx: tx}, nil
}

// Ping implements the Ping method of the Pinger interface
func (c *MySQLConnWrapper) Ping(ctx context.Context) error {
	if pinger, ok := c.conn.(driver.Pinger); ok {
		return pinger.Ping(ctx)
	}
	return nil
}

// ExecContext implements the ExecContext method of the ExecerContext interface
func (c *MySQLConnWrapper) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if execer, ok := c.conn.(driver.ExecerContext); ok {
		return execer.ExecContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

// QueryContext implements the QueryContext method of the QueryerContext interface
func (c *MySQLConnWrapper) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if queryer, ok := c.conn.(driver.QueryerContext); ok {
		return queryer.QueryContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

// PrepareContext implements the PrepareContext method of the ConnPrepareContext interface
func (c *MySQLConnWrapper) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if preparer, ok := c.conn.(driver.ConnPrepareContext); ok {
		return preparer.PrepareContext(ctx, query)
	}
	return c.Prepare(query)
}

// BeginTx implements the BeginTx method of the ConnBeginTx interface
func (c *MySQLConnWrapper) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if beginner, ok := c.conn.(driver.ConnBeginTx); ok {
		tx, _ := beginner.BeginTx(ctx, opts)
		return &MySQLTxWrapper{tx: tx}, nil
	}
	return c.Begin()
}

// ResetSession implements the ResetSession method of the SessionResetter interface
func (c *MySQLConnWrapper) ResetSession(ctx context.Context) error {
	if resetter, ok := c.conn.(driver.SessionResetter); ok {
		return resetter.ResetSession(ctx)
	}
	return nil
}

// IsValid implements the IsValid method of the Validator interface
func (c *MySQLConnWrapper) IsValid() bool {
	if validator, ok := c.conn.(driver.Validator); ok {
		return validator.IsValid()
	}
	return true
}

// MySQLStmtWrapper wraps the original MySQL statement
type MySQLStmtWrapper struct {
	stmt driver.Stmt
}

// Close wraps the Close method of the original MySQL statement
func (s *MySQLStmtWrapper) Close() error {
	return s.stmt.Close()
}

// NumInput wraps the NumInput method of the original MySQL statement
func (s *MySQLStmtWrapper) NumInput() int {
	return s.stmt.NumInput()
}

// Exec wraps the Exec method of the original MySQL statement
func (s *MySQLStmtWrapper) Exec(args []driver.Value) (driver.Result, error) {
	return s.stmt.Exec(args)
}

// ExecContext implements the ExecContext method of the StmtExecContext interface
func (s *MySQLStmtWrapper) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if execer, ok := s.stmt.(driver.StmtExecContext); ok {
		return execer.ExecContext(ctx, args)
	}
	return s.Exec(convertNamedValues(args))
}

// Query wraps the Query method of the original MySQL statement
func (s *MySQLStmtWrapper) Query(args []driver.Value) (driver.Rows, error) {
	return s.stmt.Query(args)
}

// QueryContext implements the QueryContext method of the StmtQueryContext interface
func (s *MySQLStmtWrapper) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if queryer, ok := s.stmt.(driver.StmtQueryContext); ok {
		return queryer.QueryContext(ctx, args)
	}
	return s.Query(convertNamedValues(args))
}

// MySQLTxWrapper wraps the original MySQL transaction
type MySQLTxWrapper struct {
	tx driver.Tx
}

// Commit wraps the Commit method of the original MySQL transaction
func (tx *MySQLTxWrapper) Commit() error {
	log.Printf("Committing transaction %v", tx)
	return tx.tx.Commit()
}

// Rollback wraps the Rollback method of the original MySQL transaction
func (tx *MySQLTxWrapper) Rollback() error {
	log.Printf("Rolling back transaction %v", tx)
	return tx.tx.Rollback()
}

func init() {
	dialect, _ := gorm.GetDialect("mysql")
	gorm.RegisterDialect("mysqlWrapper", dialect)
	sql.Register("mysqlWrapper", &MySQLDriverWrapper{originalDriver: &mysql.MySQLDriver{}})
}

// Helper function to convert []driver.NamedValue to []driver.Value
func convertNamedValues(named []driver.NamedValue) []driver.Value {
	values := make([]driver.Value, len(named))
	for i, nv := range named {
		values[i] = nv.Value
	}
	return values
}
