package sql

import (
	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"github.com/jmoiron/sqlx"
)

type SqlDB struct {
	db         *sqlx.DB
	connString string
}

func (m *SqlDB) GetConnectionString() string {
	return m.connString
}

// sqlContextGetter is an interface provided both by transaction and standard db connection
// type sqlContextGetter interface {
// 	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
// }

func (m *SqlDB) InitDB() error {
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

func (m *SqlDB) GetAllTokens() []types.TokenPriceResponse {
	var tokens []types.TokenPriceResponse
	return tokens
}

func (m *SqlDB) GetAllFiats() []types.FiatPriceResponse {
	var fiats []types.FiatPriceResponse
	return fiats
}

func (m *SqlDB) GetTokenNames() ([]string, error) {
	return nil, nil
}

func (m *SqlDB) GetPriceIDs() ([]string, error) {
	return nil, nil
}

func (m *SqlDB) GetPrices(from string) ([]types.Prices, error) {
	var prices []types.Prices
	return prices, nil
}

func (m *SqlDB) UpsertTokenPrice(price float64, token string) error {
	return nil
}

func (m *SqlDB) UpsertFiatPrice(price float64, token string) error {
	return nil
}

func (m *SqlDB) UpsertToken(to string, symbol string, price float64, time int64) error {
	return nil
}

func (m *SqlDB) UpsertTokenSupply(to string, symbol string, supply float64) error {
	return nil
}

func (m *SqlDB) Query(query string, args ...interface{}) (*sqlx.Rows, error) {
	q, err := m.db.Queryx(query, args...)
	if err != nil {
		return nil, err
	}
	return q, nil
}
