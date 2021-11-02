package sql

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach-go/v2/crdb/crdbsqlx"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
)

const (
	DriverPGX = "pgx"
)

// New returns an Instance connected to the database pointed by connString.
func NewDB(connString string) (*SqlDB, error) {
	return NewWithDriver(connString, DriverPGX)
}

// NewWithDriver returns an Instance connected to the database pointed by connString with the given driver.
func NewWithDriver(connString string, driver string) (*SqlDB, error) {
	db, err := sqlx.Connect(driver, connString)
	if err != nil {
		return nil, err
	}

	m := &SqlDB{
		db:         db,
		connString: connString,
	}

	if err := m.db.Ping(); err != nil {
		return nil, fmt.Errorf("cannot ping db, %w", err)
	}

	m.db.DB.SetMaxOpenConns(25)
	m.db.DB.SetMaxIdleConns(25)
	m.db.DB.SetConnMaxLifetime(5 * time.Minute)

	return m, nil
}

// Close closes the connection held by i.
func (m *SqlDB) Close() error {
	return m.db.Close()
}

// Exec executes query with the given params.
// If params is nil, query is assumed to be of the `SELECT` kind, and the resulting data will be written in dest.
func (m *SqlDB) Exec(query string, params interface{}, dest interface{}) error {
	return crdbsqlx.ExecuteTx(context.Background(), m.db, nil, func(tx *sqlx.Tx) error {
		if dest != nil {
			if params != nil {
				return tx.Select(dest, query, params)
			}

			return tx.Select(dest, query)
		}

		res, err := tx.NamedExec(query, params)
		if err != nil {
			return fmt.Errorf("transaction named exec error, %w", err)
		}

		re, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("transaction named exec error, %w", err)
		}

		if re == 0 {
			return fmt.Errorf("affected rows are zero")
		}

		return nil
	})
}
