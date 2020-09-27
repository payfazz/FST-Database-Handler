package data

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

type contextKey string

const (
	// USERCONTEXTKEY Key for username in context
	USERCONTEXTKEY contextKey = "UserID"

	// TXCONTEXTKEY Key for transaction in context
	TXCONTEXTKEY contextKey = "TXDB"
)

// Queryer represents the data commands interface
type Queryer interface {
	PrepareNamed(query string) (*sqlx.NamedStmt, error)
	Rebind(query string) string
	Select(dest interface{}, query string, args ...interface{}) error
	Get(dest interface{}, query string, args ...interface{}) error
	Prepare(query string) (*sql.Stmt, error)
	Queryx(query string, args ...interface{}) (*sqlx.Rows, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

// Manager represents the manager to manage the data consistency
type Manager struct {
	db *sqlx.DB
}

// newContext creates a new data context
func newContext(ctx context.Context, q Queryer) context.Context {
	ctx = context.WithValue(ctx, TXCONTEXTKEY, q)
	return ctx
}

// RunInTransaction runs the f with the transaction queryable inside the context
func (m *Manager) RunInTransaction(ctx context.Context, f func(tctx context.Context) error) error {
	tx, err := m.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // re-throw panic after Rollback
		} else if err != nil {
			tx.Rollback() // err is non-nil; don't change it
		} else {
			err = tx.Commit() // err is nil; if Commit returns error update err
		}
	}()

	ctx = newContext(ctx, tx)
	err = f(ctx)
	return err

}

// NewManager creates a new manager
func NewManager(db *sqlx.DB) *Manager {
	return &Manager{
		db: db,
	}
}
