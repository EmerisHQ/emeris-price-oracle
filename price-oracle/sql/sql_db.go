package sql

import (
	"github.com/jmoiron/sqlx"
)

type MySQLDB struct {
	db         *sqlx.DB
	connString string
}

func (m *MySQLDB) GetConnectionString() string {
	return m.connString
}

// sqlContextGetter is an interface provided both by transaction and standard db connection
// type sqlContextGetter interface {
// 	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
// }

func (m MySQLDB) InitDB() error {
	q, err := m.Query("SHOW TABLES FROM oracle")
	if q != nil {
		defer q.Close()
	}
	if err != nil {
		m.runMigrations()
	}

	//interim measures
	q, err = m.Query("SELECT * FROM oracle.coingecko")
	if q != nil {
		defer q.Close()
	}
	if err != nil {
		m.runMigrationsCoingecko()
	}
	return nil
}

func (m *MySQLDB) Query(query string, args ...interface{}) (*sqlx.Rows, error) {
	q, err := m.db.Queryx(query, args...)
	if err != nil {
		return nil, err
	}
	return q, nil
}
