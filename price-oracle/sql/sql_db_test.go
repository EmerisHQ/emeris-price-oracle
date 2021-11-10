package sql

import (
	"strings"
	"testing"
	"time"

	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"github.com/allinbits/emeris-price-oracle/utils/logging"
	"github.com/cockroachdb/cockroach-go/v2/testserver"
	"github.com/stretchr/testify/require"
)

func TestInit(t *testing.T) {
	testServer := setup(t)
	defer tearDown(testServer)

	connStr := testServer.PGURL().String()
	require.NotNil(t, connStr)

	mDB, err := NewDB(connStr)
	require.NoError(t, err)
	require.Equal(t, mDB.GetConnectionString(), connStr)
	defer mDB.Close()

	err = mDB.Init()
	require.NoError(t, err)

	rows, err := mDB.Query("SHOW TABLES FROM oracle")
	require.NoError(t, err)
	require.NotNil(t, rows)

	var tableCountDB int
	for rows.Next() {
		tableCountDB++
	}
	err = rows.Err()
	require.NoError(t, err)

	err = rows.Close()
	require.NoError(t, err)

	var tableCountMigration int
	for _, migrationQuery := range migrationList {
		if strings.HasPrefix(strings.TrimPrefix(migrationQuery, "\n"), "CREATE TABLE") {
			tableCountMigration++
		}
	}

	rows, _ = mDB.Query("SELECT * FROM oracle.coingecko")
	require.NotNil(t, rows)

	for rows.Next() {
		tableCountDB++
	}
	err = rows.Err()
	require.NoError(t, err)

	err = rows.Close()
	require.NoError(t, err)

	for _, migrationQueryCoingecko := range migrationCoingecko {
		if strings.HasPrefix(strings.TrimPrefix(migrationQueryCoingecko, "\n"), "CREATE TABLE") {
			tableCountMigration++
		}
	}

	require.Equal(t, tableCountMigration, tableCountDB)
}

func TestGetTokens(t *testing.T) {
	testServer := setup(t)
	defer tearDown(testServer)

	connStr := testServer.PGURL().String()
	require.NotNil(t, connStr)

	mDB, err := NewDB(connStr)
	require.NoError(t, err)
	require.Equal(t, mDB.GetConnectionString(), connStr)
	defer mDB.Close()

	err = mDB.Init()
	require.NoError(t, err)

	logger := logging.New(logging.LoggingConfig{
		LogPath: "",
		Debug:   true,
	})

	token := types.TokenPriceResponse{
		Symbol: "ATOM",
		Price:  -50,
		Supply: -100000,
	}

	err = mDB.UpsertTokenPrice(token.Price, token.Symbol, logger)
	require.NoError(t, err)

	err = mDB.UpsertTokenSupply(types.CoingeckoSupplyStore, token.Symbol, token.Supply, logger)
	require.NoError(t, err)

	selectToken := types.SelectToken{
		Tokens: []string{"ATOM"},
	}
	resp, err := mDB.GetTokens(selectToken)
	require.NoError(t, err)
	require.Equal(t, token, resp[0])
}

func TestGetFiats(t *testing.T) {
	testServer := setup(t)
	defer tearDown(testServer)

	connStr := testServer.PGURL().String()
	require.NotNil(t, connStr)

	mDB, err := NewDB(connStr)
	require.NoError(t, err)
	require.Equal(t, mDB.GetConnectionString(), connStr)
	defer mDB.Close()

	err = mDB.Init()
	require.NoError(t, err)

	logger := logging.New(logging.LoggingConfig{
		LogPath: "",
		Debug:   true,
	})

	fiat := types.FiatPriceResponse{
		Symbol: "USD",
		Price:  -1,
	}

	err = mDB.UpsertFiatPrice(fiat.Price, fiat.Symbol, logger)
	require.NoError(t, err)

	selectFiats := types.SelectFiat{
		Fiats: []string{"USD"},
	}
	resp, err := mDB.GetFiats(selectFiats)
	require.NoError(t, err)
	require.Equal(t, fiat, resp[0])
}

func TestGetTokenNames(t *testing.T) {
	testServer := setup(t)
	defer tearDown(testServer)

	connStr := testServer.PGURL().String()
	require.NotNil(t, connStr)

	mDB, err := NewDB(connStr)
	require.NoError(t, err)
	require.Equal(t, mDB.GetConnectionString(), connStr)
	defer mDB.Close()

	//build mock cns.chains table
	_, err = mDB.GetTokenNames()
	require.Error(t, err)
}

func TestGetPriceIDs(t *testing.T) {
	testServer := setup(t)
	defer tearDown(testServer)

	connStr := testServer.PGURL().String()
	require.NotNil(t, connStr)

	mDB, err := NewDB(connStr)
	require.NoError(t, err)
	require.Equal(t, mDB.GetConnectionString(), connStr)
	defer mDB.Close()

	//build mock cns.chains table
	_, err = mDB.GetPriceIDs()
	require.Error(t, err)
}

func TestGetPrices(t *testing.T) {
	testServer := setup(t)
	defer tearDown(testServer)

	connStr := testServer.PGURL().String()
	require.NotNil(t, connStr)

	mDB, err := NewDB(connStr)
	require.NoError(t, err)
	require.Equal(t, mDB.GetConnectionString(), connStr)
	defer mDB.Close()

	err = mDB.Init()
	require.NoError(t, err)

	now := time.Now()
	price := types.Prices{
		Symbol:    "ATOM",
		Price:     -100,
		UpdatedAt: now.Unix(),
	}

	tx := mDB.db.MustBegin()
	tx.MustExec("INSERT INTO oracle.binance VALUES (($1),($2),($3));", price.Symbol, price.Price, price.UpdatedAt)
	err = tx.Commit()
	require.NoError(t, err)

	prices, err := mDB.GetPrices(types.BinanceStore)
	require.NoError(t, err)
	require.Equal(t, price, prices[0])
}

func TestUpsertTokenPrice(t *testing.T) {
	testServer := setup(t)
	defer tearDown(testServer)

	connStr := testServer.PGURL().String()
	require.NotNil(t, connStr)

	mDB, err := NewDB(connStr)
	require.NoError(t, err)
	require.Equal(t, mDB.GetConnectionString(), connStr)
	defer mDB.Close()

	err = mDB.Init()
	require.NoError(t, err)

	logger := logging.New(logging.LoggingConfig{
		LogPath: "",
		Debug:   true,
	})

	price := types.TokenPriceResponse{
		Symbol: "ATOM",
		Price:  -100,
	}

	err = mDB.UpsertTokenPrice(price.Price, price.Symbol, logger)
	require.NoError(t, err)

	rows, err := mDB.Query("SELECT * FROM " + types.TokensStore)
	require.NoError(t, err)

	var symbol string
	var p float64
	var prices []types.TokenPriceResponse
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&symbol, &p)
		require.NoError(t, err)
		prices = append(prices, types.TokenPriceResponse{Symbol: symbol, Price: p})
	}
	require.Equal(t, price, prices[0])
}

func TestUpsertFiatPrice(t *testing.T) {
	testServer := setup(t)
	defer tearDown(testServer)

	connStr := testServer.PGURL().String()
	require.NotNil(t, connStr)

	mDB, err := NewDB(connStr)
	require.NoError(t, err)
	require.Equal(t, mDB.GetConnectionString(), connStr)
	defer mDB.Close()

	err = mDB.Init()
	require.NoError(t, err)

	logger := logging.New(logging.LoggingConfig{
		LogPath: "",
		Debug:   true,
	})

	price := types.FiatPriceResponse{
		Symbol: "USD",
		Price:  -1,
	}

	err = mDB.UpsertFiatPrice(price.Price, price.Symbol, logger)
	require.NoError(t, err)

	rows, err := mDB.Query("SELECT * FROM " + types.FiatsStore)
	require.NoError(t, err)

	var symbol string
	var p float64
	var prices []types.FiatPriceResponse
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&symbol, &p)
		require.NoError(t, err)
		prices = append(prices, types.FiatPriceResponse{Symbol: symbol, Price: p})
	}
	require.Equal(t, price, prices[0])
}

func TestUpsertToken(t *testing.T) {
	testServer := setup(t)
	defer tearDown(testServer)

	connStr := testServer.PGURL().String()
	require.NotNil(t, connStr)

	mDB, err := NewDB(connStr)
	require.NoError(t, err)
	require.Equal(t, mDB.GetConnectionString(), connStr)
	defer mDB.Close()

	err = mDB.Init()
	require.NoError(t, err)

	now := time.Now()
	price := types.Prices{
		Symbol:    "ATOM",
		Price:     -100,
		UpdatedAt: now.Unix(),
	}

	logger := logging.New(logging.LoggingConfig{
		LogPath: "",
		Debug:   true,
	})

	err = mDB.UpsertToken(types.BinanceStore, price.Symbol, price.Price, now.Unix(), logger)
	require.NoError(t, err)

	prices, err := mDB.GetPrices(types.BinanceStore)
	require.NoError(t, err)
	require.Equal(t, price, prices[0])
}

func TestUpsertTokenSupply(t *testing.T) {
	testServer := setup(t)
	defer tearDown(testServer)

	connStr := testServer.PGURL().String()
	require.NotNil(t, connStr)

	mDB, err := NewDB(connStr)
	require.NoError(t, err)
	require.Equal(t, mDB.GetConnectionString(), connStr)
	defer mDB.Close()

	err = mDB.Init()
	require.NoError(t, err)

	price := types.TokenPriceResponse{
		Symbol: "ATOM",
		Supply: -200,
	}

	logger := logging.New(logging.LoggingConfig{
		LogPath: "",
		Debug:   true,
	})

	err = mDB.UpsertTokenSupply(types.CoingeckoSupplyStore, price.Symbol, price.Supply, logger)
	require.NoError(t, err)

	rows, err := mDB.Query("SELECT * FROM " + types.CoingeckoSupplyStore)
	require.NoError(t, err)

	var symbol string
	var supply float64
	var prices []types.TokenPriceResponse
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&symbol, &supply)
		require.NoError(t, err)
		prices = append(prices, types.TokenPriceResponse{Symbol: symbol, Supply: supply})
	}
	require.Equal(t, price, prices[0])
}

func setup(t *testing.T) testserver.TestServer {
	ts, err := testserver.NewTestServer()
	require.NoError(t, err)
	require.NoError(t, ts.WaitForInit())

	return ts
}

func tearDown(ts testserver.TestServer) {
	ts.Stop()
}