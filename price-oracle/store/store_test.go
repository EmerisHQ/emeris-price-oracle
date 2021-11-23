package store_test

import (
	"context"
	"github.com/allinbits/emeris-price-oracle/price-oracle/config"
	"github.com/allinbits/emeris-price-oracle/price-oracle/store"
	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"github.com/allinbits/emeris-price-oracle/utils/logging"
	"go.uber.org/zap"
	"testing"
	"time"

	models "github.com/allinbits/demeris-backend-models/cns"
	cnsDB "github.com/allinbits/emeris-cns-server/cns/database"
	"github.com/allinbits/emeris-price-oracle/price-oracle/sql"
	"github.com/cockroachdb/cockroach-go/v2/testserver"
	"github.com/stretchr/testify/require"
)

func TestNewStoreHandler(t *testing.T) {
	_, _, storeHandler, _, _, tDown := setup(t)
	defer tDown()
	require.NotNil(t, storeHandler)
}

func TestCnsTokenQuery(t *testing.T) {
	_, cancel, storeHandler, _, _, tDown := setup(t)
	defer tDown()
	defer cancel()

	whiteList, err := storeHandler.CnsTokenQuery()
	require.NoError(t, err)
	require.NotNil(t, whiteList)

	require.Equal(t, []string{"ATOM", "LUNA"}, whiteList)
}

func TestCnsPriceIdQuery(t *testing.T) {
	_, cancel, storeHandler, _, _, tDown := setup(t)
	defer tDown()
	defer cancel()

	whiteList, err := storeHandler.CnsPriceIdQuery()
	require.NoError(t, err)
	require.NotNil(t, whiteList)

	require.Equal(t, []string{"cosmos", "terra-luna"}, whiteList)
}

func TestPriceTokenAggregator(t *testing.T) {
	_, cancel, storeHandler, logger, cfg, tDown := setup(t)
	defer tDown()
	defer cancel()

	tokens := types.SelectToken{
		Tokens: []string{"ATOMUSDT", "LUNAUSDT"},
	}
	stores := []string{store.BinanceStore, store.CoingeckoStore}

	for _, tk := range tokens.Tokens {
		for i, s := range stores {
			err := storeHandler.Store.UpsertToken(s, tk, float64(10+i), time.Now().Unix(), logger)
			require.NoError(t, err)
		}
	}

	err := storeHandler.PriceTokenAggregator(logger, cfg)
	require.NoError(t, err)

	prices, err := storeHandler.Store.GetTokens(tokens)
	require.NoError(t, err)

	for i, p := range prices {
		require.Equal(t, tokens.Tokens[i], p.Symbol)
		require.Equal(t, 10.5, p.Price)
	}
}

func TestPriceFiatAggregator(t *testing.T) {
	_, cancel, storeHandler, logger, cfg, tDown := setup(t)
	defer tDown()
	defer cancel()

	fiats := types.SelectFiat{
		Fiats: []string{"USDCHF", "USDEUR", "USDKRW"},
	}
	stores := []string{store.FixerStore}

	for _, tk := range fiats.Fiats {
		for i, s := range stores {
			err := storeHandler.Store.UpsertToken(s, tk, float64(10+i), time.Now().Unix(), logger)
			require.NoError(t, err)
		}
	}

	err := storeHandler.PriceFiatAggregator(logger, cfg)
	require.NoError(t, err)

	prices, err := storeHandler.Store.GetFiats(fiats)
	require.NoError(t, err)
	require.NotNil(t, prices)

	for i, p := range prices {
		require.Equal(t, fiats.Fiats[i], p.Symbol)
		require.Equal(t, float64(10), p.Price)
	}
}

func getStoreHandler(t *testing.T, ts testserver.TestServer) (*store.Handler, error) {
	t.Helper()
	db, err := sql.NewDB(ts.PGURL().String())
	if err != nil {
		return nil, err
	}

	storeHandler, err := store.NewStoreHandler(db)
	if err != nil {
		return nil, err
	}

	return storeHandler, nil
}

func setup(t *testing.T) (context.Context, func(), *store.Handler, *zap.SugaredLogger, *config.Config, func()) {
	t.Helper()
	ts, err := testserver.NewTestServer()
	require.NoError(t, err)
	require.NoError(t, ts.WaitForInit())
	connStr := ts.PGURL().String()
	insertToken(t, connStr)

	handler, err := getStoreHandler(t, ts)
	require.NoError(t, err)
	require.NotNil(t, handler)

	cfg := &config.Config{ // config.Read() is not working. Fixing is not in scope of this task. That comes later.
		LogPath:               "",
		Debug:                 true,
		DatabaseConnectionURL: connStr,
		Interval:              "10s",
		Whitelistfiats:        []string{"EUR", "KRW", "CHF"},
	}

	logger := logging.New(logging.LoggingConfig{
		LogPath: cfg.LogPath,
		Debug:   cfg.Debug,
	})
	ctx, cancel := context.WithCancel(context.Background())
	return ctx, cancel, handler, logger, cfg, func() { ts.Stop() }
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
