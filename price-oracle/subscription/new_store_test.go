package subscription_test

import (
	"testing"
	"time"

	models "github.com/allinbits/demeris-backend-models/cns"
	cnsDB "github.com/allinbits/emeris-cns-server/cns/database"
	"github.com/allinbits/emeris-price-oracle/price-oracle/sql"
	"github.com/allinbits/emeris-price-oracle/price-oracle/subscription"
	"github.com/cockroachdb/cockroach-go/v2/testserver"
	"github.com/stretchr/testify/require"
)

func TestNewstoreHandler(t *testing.T) {
	testServer := setup(t)
	defer tearDown(testServer)

	storeHandler, err := getStoreHandler(t, testServer)
	require.NoError(t, err)
	require.NotNil(t, storeHandler)
}

func TestCnsTokenQuery(t *testing.T) {
	testServer := setup(t)
	defer tearDown(testServer)

	storeHandler, err := getStoreHandler(t, testServer)
	require.NoError(t, err)
	require.NotNil(t, storeHandler.Store)

	insertToken(t, testServer.PGURL().String())

	whiteList, err := storeHandler.CnsTokenQuery()
	require.NoError(t, err)
	require.NotNil(t, whiteList)

	require.Equal(t, []string{"ATOM", "LUNA"}, whiteList)
}

func TestCnsPriceIdQuery(t *testing.T) {
	testServer := setup(t)
	defer tearDown(testServer)

	storeHandler, err := getStoreHandler(t, testServer)
	require.NoError(t, err)
	require.NotNil(t, storeHandler)

	insertToken(t, testServer.PGURL().String())

	whiteList, err := storeHandler.CnsPriceIdQuery()
	require.NoError(t, err)
	require.NotNil(t, whiteList)

	require.Equal(t, []string{"cosmos", "terra-luna"}, whiteList)
}

func getdb(t *testing.T, ts testserver.TestServer) (*sql.SqlDB, error) {
	connStr := ts.PGURL().String()
	return sql.NewDB(connStr)
}

func getStoreHandler(t *testing.T, ts testserver.TestServer) (*subscription.StoreHandler, error) {
	db, err := getdb(t, ts)
	if err != nil {
		return nil, err
	}

	storeHandler, err := subscription.NewStoreHandler(db)
	if err != nil {
		return nil, err
	}

	return storeHandler, nil
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

func insertToken(t *testing.T, connStr string) {
	chain := models.Chain{
		ChainName:        "cosmos-hub",
		DemerisAddresses: []string{"addr1"},
		DisplayName:      "ATOM display name",
		GenesisHash:      "hash",
		NodeInfo:         models.NodeInfo{},
		ValidBlockThresh: models.Threshold(1 * time.Second),
		DerivationPath:   "derivation_path",
		SupportedWallets: []string{"metamask"},
		Logo:             "logo 1",
		Denoms: []models.Denom{
			{
				Name:        "ATOM",
				PriceID:     "cosmos",
				DisplayName: "ATOM",
				FetchPrice:  true,
				Ticker:      "ATOM",
			},
			{
				Name:        "LUNA",
				PriceID:     "terra-luna",
				DisplayName: "LUNA",
				FetchPrice:  true,
				Ticker:      "LUNA",
			},
		},
		PrimaryChannel: models.DbStringMap{
			"cosmos-hub":  "ch0",
			"persistence": "ch2",
		},
	}
	cnsInstanceDB, err := cnsDB.New(connStr)
	require.NoError(t, err)

	err = cnsInstanceDB.AddChain(chain)
	require.NoError(t, err)

	cc, err := cnsInstanceDB.Chains()
	require.NoError(t, err)
	require.Equal(t, 1, len(cc))
}
