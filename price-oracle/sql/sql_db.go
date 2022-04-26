package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/emerishq/emeris-price-oracle/price-oracle/store"
	"github.com/getsentry/sentry-go"

	"github.com/cockroachdb/cockroach-go/v2/crdb/crdbsqlx"
	"github.com/emerishq/emeris-price-oracle/price-oracle/types"
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

func (m *SqlDB) Init(ctx context.Context) error {
	q, err := m.db.QueryxContext(ctx, "SHOW TABLES FROM oracle")
	if err != nil {
		if strings.Contains(err.Error(), "target database or schema does not exist") {
			if err = m.createDatabase(ctx); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	if q != nil {
		if err = q.Close(); err != nil {
			return err
		}
	}

	if err = m.runMigrations(ctx); err != nil {
		return err
	}

	return nil
}

func (m *SqlDB) GetTokenPriceAndSupplies(ctx context.Context, tokens []string) ([]types.TokenPriceAndSupply, error) {
	defer sentry.StartSpan(ctx, "db.GetTokenPriceAndSupplies").Finish()

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

	rows, err := m.db.QueryxContext(ctx, query, symbolList...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		if err := rows.Scan(&symbol, &price); err != nil {
			return nil, err
		}
		rowGeckoSupply, err := m.db.QueryxContext(ctx, "SELECT * FROM "+store.CoingeckoSupplyStore+" WHERE symbol=$1", symbol)
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

func (m *SqlDB) GetFiatPrices(ctx context.Context, fiats []string) ([]types.FiatPrice, error) {
	defer sentry.StartSpan(ctx, "db.GetFiatPrices").Finish()

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
	rows, err := m.db.QueryxContext(ctx, query, symbolList...)
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

func (m *SqlDB) GetTokenNames(ctx context.Context) ([]string, error) {
	defer sentry.StartSpan(ctx, "db.GetTokenNames").Finish()

	var whitelists []string
	q, err := m.db.QueryxContext(ctx, "SELECT  y.x->'ticker',y.x->'fetch_price' FROM cns.chains jt, LATERAL (SELECT json_array_elements(jt.denoms) x) y")
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

// GetPriceIDToTicker returns all not null price_ids with their ticker
// Returns map price_id -> ticker; Ex: cosmos -> atom; osmosis -> osmo
func (m *SqlDB) GetPriceIDToTicker(ctx context.Context) (map[string]string, error) {
	defer sentry.StartSpan(ctx, "db.GetPriceIDToTicker").Finish()

	priceIDtoTicker := make(map[string]string)
	seen := make(map[string]bool)
	q, err := m.db.QueryxContext(ctx, "SELECT  y.x->'ticker',y.x->'price_id' FROM cns.chains jt, LATERAL (SELECT json_array_elements(jt.denoms) x) y")
	if err != nil {
		return nil, err
	}
	for q.Next() {
		var priceId sql.NullString
		var ticker sql.NullString
		if err := q.Scan(&ticker, &priceId); err != nil {
			return nil, err
		}

		// If price_id is null; skip.
		if !priceId.Valid {
			continue
		}
		pid := strings.ToLower(strings.Trim(priceId.String, "\"")) // Better safe than sorry
		// CNS DB can have the same price_id multiple time.
		// It's not ideal from CNS, but can happen, so we write defencive code anyway.
		// If multiple occurrences of same price_id is found, only take the first one.
		if _, ok := seen[pid]; ok {
			continue
		}
		seen[pid] = true

		// However, if ticker is null, just have an empty placeholder.
		if !ticker.Valid {
			ticker.String = ""
		}
		tkr := strings.ToLower(strings.Trim(ticker.String, "\""))
		priceIDtoTicker[pid] = tkr
	}
	return priceIDtoTicker, nil
}

func (m *SqlDB) GetPrices(ctx context.Context, from string) ([]types.Prices, error) {
	defer sentry.StartSpan(ctx, "db.GetPrices").Finish()

	var prices []types.Prices //nolint:prealloc
	var price types.Prices
	rows, err := m.db.QueryxContext(ctx, "SELECT * FROM "+from)
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

func (m *SqlDB) UpsertPrice(ctx context.Context, to string, price float64, token string) error {
	defer sentry.StartSpan(ctx, "db.UpsertPrice").Finish()

	tx, err := m.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, "UPDATE "+to+" SET price = ($1) WHERE symbol = ($2)", price, token)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	// If you perform an update without a token column, it does not respond as an error; it responds with zero.
	// So you have to insert a new one in the column.
	if rowsAffected == 0 {
		_, err := tx.ExecContext(ctx, "INSERT INTO "+to+" VALUES (($1),($2));", token, price)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (m *SqlDB) UpsertToken(ctx context.Context, to string, symbol string, price float64, time int64) error {
	defer sentry.StartSpan(ctx, "db.UpsertToken").Finish()

	tx, err := m.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, "UPDATE "+to+" SET price = ($1),updatedat = ($2) WHERE symbol = ($3)", price, time, symbol)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		_, err := tx.ExecContext(ctx, "INSERT INTO "+to+" VALUES (($1),($2),($3));", symbol, price, time)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (m *SqlDB) UpsertTokenSupply(ctx context.Context, to string, symbol string, supply float64) error {
	defer sentry.StartSpan(ctx, "db.UpsertTokenSupply").Finish()

	tx, err := m.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, "UPDATE "+to+" SET supply = ($1) WHERE symbol = ($2)", supply, symbol)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		_, err := tx.ExecContext(ctx, "INSERT INTO "+to+" VALUES (($1),($2));", symbol, supply)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (m *SqlDB) Query(query string, args ...interface{}) (*sqlx.Rows, error) {
	return m.db.Queryx(query, args...)
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
