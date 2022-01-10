package sql

import (
	"strings"
	"testing"
	"time"

	"github.com/allinbits/emeris-price-oracle/price-oracle/store"
	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"github.com/cockroachdb/cockroach-go/v2/testserver"
	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T) {
	testServer := setup(t)
	defer tearDown(testServer)

	connStr := testServer.PGURL().String()
	require.NotNil(t, connStr)

	mDB, err := NewDB(connStr)
	require.NoError(t, err)
	require.Equal(t, mDB.GetConnectionString(), connStr)
	defer func() {
		err = mDB.Close()
		require.NoError(t, err)
	}()

	err = mDB.Init()
	require.NoError(t, err)

	store.TestStore(t, mDB)
}

func TestInit(t *testing.T) {
	testServer := setup(t)
	defer tearDown(testServer)

	connStr := testServer.PGURL().String()
	require.NotNil(t, connStr)

	mDB, err := NewDB(connStr)
	require.NoError(t, err)
	require.Equal(t, mDB.GetConnectionString(), connStr)
	defer func() {
		err = mDB.Close()
		require.NoError(t, err)
	}()

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
	defer func() {
		err = mDB.Close()
		require.NoError(t, err)
	}()

	err = mDB.Init()
	require.NoError(t, err)

	token := types.TokenPriceAndSupply{
		Symbol: "ATOM",
		Price:  -50,
		Supply: -100000,
	}

	err = mDB.UpsertPrice(store.TokensStore, token.Price, token.Symbol)
	require.NoError(t, err)

	err = mDB.UpsertTokenSupply(store.CoingeckoSupplyStore, token.Symbol, token.Supply)
	require.NoError(t, err)

	resp, err := mDB.GetTokenPriceAndSupplies([]string{"ATOM"})
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
	defer func() {
		err = mDB.Close()
		require.NoError(t, err)
	}()

	err = mDB.Init()
	require.NoError(t, err)

	fiat := types.FiatPrice{
		Symbol: "USD",
		Price:  -1,
	}

	err = mDB.UpsertPrice(store.FiatsStore, fiat.Price, fiat.Symbol)
	require.NoError(t, err)

	resp, err := mDB.GetFiatPrices([]string{"USD"})
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
	defer func() {
		err = mDB.Close()
		require.NoError(t, err)
	}()

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
	defer func() {
		err = mDB.Close()
		require.NoError(t, err)
	}()

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
	defer func() {
		err = mDB.Close()
		require.NoError(t, err)
	}()

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

	prices, err := mDB.GetPrices(store.BinanceStore)
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
	defer func() {
		err = mDB.Close()
		require.NoError(t, err)
	}()

	err = mDB.Init()
	require.NoError(t, err)

	price := types.TokenPriceAndSupply{
		Symbol: "ATOM",
		Price:  -100,
	}

	err = mDB.UpsertPrice(store.TokensStore, price.Price, price.Symbol)
	require.NoError(t, err)

	rows, err := mDB.Query("SELECT * FROM " + store.TokensStore)
	require.NoError(t, err)

	var symbol string
	var p float64
	var prices []types.TokenPriceAndSupply
	for rows.Next() {
		err = rows.Scan(&symbol, &p)
		require.NoError(t, err)
		prices = append(prices, types.TokenPriceAndSupply{Symbol: symbol, Price: p})
	}
	err = rows.Close()
	require.NoError(t, err)

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
	defer func() {
		err = mDB.Close()
		require.NoError(t, err)
	}()

	err = mDB.Init()
	require.NoError(t, err)

	price := types.FiatPrice{
		Symbol: "USD",
		Price:  -1,
	}

	err = mDB.UpsertPrice(store.FiatsStore, price.Price, price.Symbol)
	require.NoError(t, err)

	rows, err := mDB.Query("SELECT * FROM " + store.FiatsStore)
	require.NoError(t, err)

	var symbol string
	var p float64
	var prices []types.FiatPrice
	for rows.Next() {
		err = rows.Scan(&symbol, &p)
		require.NoError(t, err)
		prices = append(prices, types.FiatPrice{Symbol: symbol, Price: p})
	}
	err = rows.Close()
	require.NoError(t, err)

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
	defer func() {
		err = mDB.Close()
		require.NoError(t, err)
	}()

	err = mDB.Init()
	require.NoError(t, err)

	now := time.Now()
	price := types.Prices{
		Symbol:    "ATOM",
		Price:     -100,
		UpdatedAt: now.Unix(),
	}

	err = mDB.UpsertToken(store.BinanceStore, price.Symbol, price.Price, now.Unix())
	require.NoError(t, err)

	prices, err := mDB.GetPrices(store.BinanceStore)
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
	defer func() {
		err = mDB.Close()
		require.NoError(t, err)
	}()

	err = mDB.Init()
	require.NoError(t, err)

	price := types.TokenPriceAndSupply{
		Symbol: "ATOM",
		Supply: -200,
	}

	err = mDB.UpsertTokenSupply(store.CoingeckoSupplyStore, price.Symbol, price.Supply)
	require.NoError(t, err)

	rows, err := mDB.Query("SELECT * FROM " + store.CoingeckoSupplyStore)
	require.NoError(t, err)

	var symbol string
	var supply float64
	var prices []types.TokenPriceAndSupply

	for rows.Next() {
		err = rows.Scan(&symbol, &supply)
		require.NoError(t, err)
		prices = append(prices, types.TokenPriceAndSupply{Symbol: symbol, Supply: supply})
	}
	err = rows.Close()
	require.NoError(t, err)

	require.Equal(t, price, prices[0])
}

func TestGetGeckoId(t *testing.T) {
	testServer := setup(t)
	defer tearDown(testServer)

	connStr := testServer.PGURL().String()
	require.NotNil(t, connStr)

	mDB, err := NewDB(connStr)
	require.NoError(t, err)
	require.Equal(t, mDB.GetConnectionString(), connStr)
	defer func() {
		err = mDB.Close()
		require.NoError(t, err)
	}()

	err = mDB.Init()
	require.NoError(t, err)

	ids, err := mDB.GetGeckoId([]string{"ATOM", "LUNA"}, nil)
	require.NoError(t, err)
	t.Logf("%+v\n", ids)
}

func setup(t *testing.T) testserver.TestServer {
	t.Helper()
	ts, err := testserver.NewTestServer()
	require.NoError(t, err)
	require.NoError(t, ts.WaitForInit())

	return ts
}

func tearDown(ts testserver.TestServer) {
	ts.Stop()
}
