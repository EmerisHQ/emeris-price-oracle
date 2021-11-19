package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/allinbits/emeris-price-oracle/price-oracle/database"
	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"github.com/cockroachdb/cockroach-go/v2/crdb/crdbsqlx"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

const (
	DriverPGX = "pgx"
)

type SqlDB struct {
	db         *sqlx.DB
	connString string
}

func (m *SqlDB) GetConnectionString() string {
	return m.connString
}

func (m *SqlDB) Init() error {
	q, err := m.Query("SHOW TABLES FROM oracle")
	if err != nil {
		if err = m.runMigrations(); err != nil {
			return err
		}
	}
	if q != nil {
		defer q.Close()
	}

	//interim measures
	q, err = m.Query("SELECT * FROM oracle.coingecko")
	if err != nil {
		if err = m.runMigrationsCoingecko(); err != nil {
			return err
		}
	}
	if q != nil {
		defer q.Close()
	}
	return nil
}

func (m *SqlDB) GetTokens(selectToken types.SelectToken) ([]types.TokenPriceResponse, error) {
	var tokens []types.TokenPriceResponse
	var token types.TokenPriceResponse
	var symbolList []interface{}

	symbolNum := len(selectToken.Tokens)

	query := "SELECT * FROM " + database.TokensStore + " WHERE symbol=$1"

	for i := 2; i <= symbolNum; i++ {
		query += " OR" + " symbol=$" + strconv.Itoa(i)
	}

	for _, usersymbol := range selectToken.Tokens {
		symbolList = append(symbolList, usersymbol)
	}

	rows, err := m.Query(query, symbolList...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var symbol string
		var price float64
		var supply float64

		if err := rows.Scan(&symbol, &price); err != nil {
			return nil, err
		}
		//rowCmcSupply, err := r.s.d.Query("SELECT * FROM oracle.coinmarketcapsupply WHERE symbol=$1", symbol)
		rowCmcSupply, err := m.Query("SELECT * FROM "+database.CoingeckoSupplyStore+" WHERE symbol=$1", symbol)
		if err != nil {
			return nil, err
		}
		defer rowCmcSupply.Close()
		for rowCmcSupply.Next() {
			if err := rowCmcSupply.Scan(&symbol, &supply); err != nil {
				return nil, err
			}
		}
		token.Symbol = symbol
		token.Price = price
		token.Supply = supply

		tokens = append(tokens, token)
	}

	return tokens, nil
}

func (m *SqlDB) GetFiats(selectFiat types.SelectFiat) ([]types.FiatPriceResponse, error) {
	var symbols []types.FiatPriceResponse
	var symbol types.FiatPriceResponse
	var symbolList []interface{}

	symbolNum := len(selectFiat.Fiats)

	query := "SELECT * FROM " + database.FiatsStore + " WHERE symbol=$1"

	for i := 2; i <= symbolNum; i++ {
		query += " OR" + " symbol=$" + strconv.Itoa(i)
	}

	for _, usersymbol := range selectFiat.Fiats {
		symbolList = append(symbolList, usersymbol)
	}

	rows, err := m.Query(query, symbolList...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {

		if err := rows.StructScan(&symbol); err != nil {
			return nil, err
		}
		symbols = append(symbols, symbol)
	}

	return symbols, nil
}

func (m *SqlDB) GetTokenNames() ([]string, error) {
	var whitelists []string
	q, err := m.Query("SELECT  y.x->'ticker',y.x->'fetch_price' FROM cns.chains jt, LATERAL (SELECT json_array_elements(jt.denoms) x) y")
	if err != nil {
		return nil, err
	}
	for q.Next() {
		var ticker string
		var fetch_price bool

		if err := q.Scan(&ticker, &fetch_price); err != nil {
			return nil, err
		}
		if fetch_price {
			ticker = strings.TrimRight(ticker, "\"")
			ticker = strings.TrimLeft(ticker, "\"")
			whitelists = append(whitelists, ticker)
		}
	}
	return whitelists, nil
}

func (m *SqlDB) GetPriceIDs() ([]string, error) {
	var whitelists []string
	q, err := m.Query("SELECT  y.x->'price_id',y.x->'fetch_price' FROM cns.chains jt, LATERAL (SELECT json_array_elements(jt.denoms) x) y")
	if err != nil {
		return nil, err
	}
	for q.Next() {
		var price_id sql.NullString
		var fetch_price bool

		if err := q.Scan(&price_id, &fetch_price); err != nil {
			return nil, err
		}
		if price_id.Valid {
			if fetch_price {
				price_id.String = strings.TrimRight(price_id.String, "\"")
				price_id.String = strings.TrimLeft(price_id.String, "\"")
				whitelists = append(whitelists, price_id.String)
			}
		} else {
			continue
		}
	}
	return whitelists, nil
}

func (m *SqlDB) GetPrices(from string) ([]types.Prices, error) {
	var prices []types.Prices
	var price types.Prices
	rows, err := m.Query("SELECT * FROM " + from)
	if err != nil {
		return nil, fmt.Errorf("fatal: GetPrices: %w, duration:%s", err, time.Second)
	}
	defer rows.Close()
	for rows.Next() {

		if err := rows.StructScan(&price); err != nil {
			return nil, fmt.Errorf("fatal: GetPrices: %w, duration:%s", err, time.Second)
		}
		prices = append(prices, price)
	}
	return prices, nil
}

func (m *SqlDB) UpsertPrice(to string, price float64, token string, logger *zap.SugaredLogger) error {
	tx := m.db.MustBegin()

	result := tx.MustExec("UPDATE "+to+" SET price = ($1) WHERE symbol = ($2)", price, token)
	updateresult, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("DB update: %w", err)
	}
	//If you perform an update without a token column, it does not respond as an error; it responds with zero.
	//So you have to insert a new one in the column.
	if updateresult == 0 {
		tx.MustExec("INSERT INTO "+to+" VALUES (($1),($2));", token, price)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("DB commit: %w", err)
	}
	logger.Infow("Insert to ", token, price)
	return nil
}

func (m *SqlDB) UpsertToken(to string, symbol string, price float64, time int64, logger *zap.SugaredLogger) error {
	tx := m.db.MustBegin()
	result := tx.MustExec("UPDATE "+to+" SET price = ($1),updatedat = ($2) WHERE symbol = ($3)", price, time, symbol)

	updateresult, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("UpsertToken DB UPDATE: %w", err)
	}
	if updateresult == 0 {
		tx.MustExec("INSERT INTO "+to+" VALUES (($1),($2),($3));", symbol, price, time)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("UpsertToken DB commit: %w", err)
	}
	return nil
}

func (m *SqlDB) UpsertTokenSupply(to string, symbol string, supply float64, logger *zap.SugaredLogger) error {
	tx := m.db.MustBegin()
	resultsupply := tx.MustExec("UPDATE "+to+" SET supply = ($1) WHERE symbol = ($2)", supply, symbol)

	updateresultsupply, err := resultsupply.RowsAffected()
	if err != nil {
		return fmt.Errorf("UpsertTokenSupply DB UPDATE: %w", err)
	}
	if updateresultsupply == 0 {
		tx.MustExec("INSERT INTO "+to+" VALUES (($1),($2));", symbol, supply)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("UpsertTokenSupply DB commit: %w", err)
	}
	return nil
}

func (m *SqlDB) Query(query string, args ...interface{}) (*sqlx.Rows, error) {
	q, err := m.db.Queryx(query, args...)
	if err != nil {
		return nil, err
	}
	return q, nil
}

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
