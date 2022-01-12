package sql

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/allinbits/emeris-price-oracle/price-oracle/store"

	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"github.com/cockroachdb/cockroach-go/v2/crdb/crdbsqlx"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
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
		if err = q.Close(); err != nil {
			return err
		}
	}

	// interim measures
	q, err = m.Query("SELECT * FROM oracle.coingecko")
	if err != nil {
		if err = m.runMigrationsCoingecko(); err != nil {
			return err
		}
	}
	if q != nil {
		if err = q.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (m *SqlDB) GetTokenPriceAndSupplies(tokens []string) ([]types.TokenPriceAndSupply, error) {
	query := "SELECT * FROM " + store.TokensStore + " WHERE symbol=$1"
	for i := 2; i <= len(tokens); i++ {
		query += " OR" + " symbol=$" + strconv.Itoa(i)
	}

	symbolList := make([]interface{}, 0, len(tokens))
	for _, symbol := range tokens {
		symbolList = append(symbolList, symbol)
	}

	var priceAndSupplies []types.TokenPriceAndSupply //nolint:prealloc
	var symbol string
	var price float64
	var supply float64

	rows, err := m.Query(query, symbolList...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		if err := rows.Scan(&symbol, &price); err != nil {
			return nil, err
		}
		rowGeckoSupply, err := m.Query("SELECT * FROM "+store.CoingeckoSupplyStore+" WHERE symbol=$1", symbol)
		if err != nil {
			return nil, err
		}
		for rowGeckoSupply.Next() {
			if err := rowGeckoSupply.Scan(&symbol, &supply); err != nil {
				return nil, err
			}
		}
		if err = rowGeckoSupply.Close(); err != nil {
			return nil, err
		}

		priceAndSupplies = append(priceAndSupplies, types.TokenPriceAndSupply{
			Symbol: symbol,
			Price:  price,
			Supply: supply,
		})
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}

	return priceAndSupplies, nil
}

func (m *SqlDB) GetFiatPrices(fiats []string) ([]types.FiatPrice, error) {
	query := "SELECT * FROM " + store.FiatsStore + " WHERE symbol=$1"
	for i := 2; i <= len(fiats); i++ {
		query += " OR" + " symbol=$" + strconv.Itoa(i)
	}

	symbolList := make([]interface{}, 0, len(fiats))
	for _, fiat := range fiats {
		symbolList = append(symbolList, fiat)
	}

	var fiatPrices []types.FiatPrice //nolint:prealloc
	var price types.FiatPrice
	rows, err := m.Query(query, symbolList...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		if err := rows.StructScan(&price); err != nil {
			return nil, err
		}
		fiatPrices = append(fiatPrices, price)
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}

	return fiatPrices, nil
}

func (m *SqlDB) GetTokenNames() ([]string, error) {
	var whitelists []string
	q, err := m.Query("SELECT  y.x->'ticker',y.x->'fetch_price' FROM cns.chains jt, LATERAL (SELECT json_array_elements(jt.denoms) x) y")
	if err != nil {
		return nil, err
	}
	for q.Next() {
		var ticker string
		var fetchPrice bool

		if err := q.Scan(&ticker, &fetchPrice); err != nil {
			return nil, err
		}
		if fetchPrice {
			ticker = strings.TrimRight(ticker, "\"")
			ticker = strings.TrimLeft(ticker, "\"")
			whitelists = append(whitelists, ticker)
		}
	}
	return whitelists, nil
}

func (m *SqlDB) GetPriceIDs() ([]string, error) {
	whitelists := make([]string, 0)
	q, err := m.Query("SELECT  y.x->'price_id',y.x->'ticker',y.x->'fetch_price' FROM cns.chains jt, LATERAL (SELECT json_array_elements(jt.denoms) x) y")
	if err != nil {
		return nil, err
	}
	for q.Next() {
		var priceId sql.NullString
		var ticker string
		var fetchPrice bool

		if err := q.Scan(&priceId, &ticker, &fetchPrice); err != nil {
			return nil, err
		}
		if !fetchPrice {
			continue
		}
		tokenIdOrTicker := strings.Trim(ticker, "\"")
		// If price_id is not null use that.
		if priceId.Valid {
			tokenIdOrTicker = strings.Trim(priceId.String, "\"")
		}
		whitelists = append(whitelists, tokenIdOrTicker)
	}
	return whitelists, nil
}

func (m *SqlDB) GetPrices(from string) ([]types.Prices, error) {
	var prices []types.Prices //nolint:prealloc
	var price types.Prices
	rows, err := m.Query("SELECT * FROM " + from)
	if err != nil {
		return nil, fmt.Errorf("fatal: GetPrices: %w", err)
	}
	for rows.Next() {

		if err := rows.StructScan(&price); err != nil {
			return nil, fmt.Errorf("fatal: GetPrices: %w", err)
		}
		prices = append(prices, price)
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	return prices, nil
}

func (m *SqlDB) GetGeckoId(names []string, client *http.Client) (map[string]string, error) {
	if len(names) == 0 {
		return nil, nil
	}
	fmt.Println(client)
	// Coin-gecko is slow for this query. First check if we have everything already in the DB.
	rows, err := m.Query("SELECT * FROM oracle.geckopriceid")
	if err != nil {
		fmt.Println(err)
	}
	var existingNameAndID map[string]string
	for rows.Next() {
		var name, geckoId string
		if err := rows.Scan(&name, &geckoId); err != nil {
			return nil, fmt.Errorf("err while scanning result %w", err)
		}
		existingNameAndID[name] = geckoId
	}
	return existingNameAndID, err
	//fmt.Println("Existing Ids:", existingNameAndID)
	//// Check DB has everything already.
	//var retMap map[string]string
	//for _, name := range names {
	//	if id, ok := existingNameAndID[name]; ok {
	//		retMap[name] = id
	//		continue
	//	}
	//	retMap = nil
	//	break
	//}
	//if len(retMap) == len(names) {
	//	return retMap, nil
	//}
	//
	//geckoClient := gecko.NewClient(client)
	//coinList, err := geckoClient.CoinsList()
	//if err != nil {
	//	return nil, err
	//}
	//var nameAndID map[string]string
	//for _, coin := range coinList {
	//	nameAndID[coin.Symbol] = coin.ID
	//}
	//
	//return nil, nil
}

func (m *SqlDB) UpsertPrice(to string, price float64, token string) error {
	tx := m.db.MustBegin()

	result := tx.MustExec("UPDATE "+to+" SET price = ($1) WHERE symbol = ($2)", price, token)
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("DB update: %w", err)
	}
	// If you perform an update without a token column, it does not respond as an error; it responds with zero.
	// So you have to insert a new one in the column.
	if rowsAffected == 0 {
		tx.MustExec("INSERT INTO "+to+" VALUES (($1),($2));", token, price)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("DB commit: %w", err)
	}
	return nil
}

func (m *SqlDB) UpsertToken(to string, symbol string, price float64, time int64) error {
	tx := m.db.MustBegin()
	result := tx.MustExec("UPDATE "+to+" SET price = ($1),updatedat = ($2) WHERE symbol = ($3)", price, time, symbol)

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("UpsertToken DB UPDATE: %w", err)
	}
	if rowsAffected == 0 {
		tx.MustExec("INSERT INTO "+to+" VALUES (($1),($2),($3));", symbol, price, time)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("UpsertToken DB commit: %w", err)
	}
	return nil
}

func (m *SqlDB) UpsertTokenSupply(to string, symbol string, supply float64) error {
	tx := m.db.MustBegin()
	result := tx.MustExec("UPDATE "+to+" SET supply = ($1) WHERE symbol = ($2)", supply, symbol)

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("UpsertTokenSupply DB UPDATE: %w", err)
	}
	if rowsAffected == 0 {
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

// NewDB returns an Instance connected to the database pointed by connString.
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

// Close closes the connection held by m.
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
