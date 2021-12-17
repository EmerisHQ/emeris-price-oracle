package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/allinbits/emeris-price-oracle/price-oracle/config"
	"github.com/allinbits/emeris-price-oracle/price-oracle/store"
	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"github.com/allinbits/emeris-price-oracle/utils/logging"
	"go.uber.org/zap"

	models "github.com/allinbits/demeris-backend-models/cns"
	cnsDB "github.com/allinbits/emeris-cns-server/cns/database"
	"github.com/allinbits/emeris-price-oracle/price-oracle/sql"
	"github.com/cockroachdb/cockroach-go/v2/testserver"
	"github.com/stretchr/testify/require"
)

func TestNewStoreHandler(t *testing.T) {
	_, _, storeHandler, tDown := setup(t)
	defer tDown()
	require.NotNil(t, storeHandler)

	require.Nil(t, storeHandler.Cache.Whitelist)
	require.Nil(t, storeHandler.Cache.FiatPrices)
	require.Nil(t, storeHandler.Cache.TokenPriceAndSupplies)

	_, err := storeHandler.GetCNSWhitelistedTokens()
	require.NoError(t, err)
	require.NotNil(t, storeHandler.Cache.Whitelist)
	require.Eventually(t, func() bool { return storeHandler.Cache.Whitelist == nil }, 10*time.Second, 1*time.Second)

	_, fiats, err := upsertFiats(storeHandler)
	require.NoError(t, err)

	_, err = storeHandler.GetFiatPrices(fiats)
	require.NoError(t, err)
	require.NotNil(t, storeHandler.Cache.FiatPrices)
	require.Eventually(t, func() bool { return storeHandler.Cache.FiatPrices == nil }, 10*time.Second, 1*time.Second)

	_, tokens, err := upsertTokens(storeHandler)
	require.NoError(t, err)

	_, err = storeHandler.GetTokenPriceAndSupplies(tokens)
	require.NoError(t, err)
	require.NotNil(t, storeHandler.Cache.TokenPriceAndSupplies)
	require.Eventually(t, func() bool { return storeHandler.Cache.TokenPriceAndSupplies == nil }, 10*time.Second, 1*time.Second)

}

func TestGetCNSWhitelistedTokens(t *testing.T) {
	_, cancel, storeHandler, tDown := setup(t)
	defer tDown()
	defer cancel()

	whiteList := []string{"ATOM", "LUNA"}

	require.Nil(t, storeHandler.Cache.Whitelist)

	whiteListFromStore, err := storeHandler.GetCNSWhitelistedTokens()
	require.NoError(t, err)

	require.Equal(t, whiteList, whiteListFromStore)

	require.NotNil(t, storeHandler.Cache.Whitelist)

	whiteListFromCache, err := storeHandler.GetCNSWhitelistedTokens()
	require.NoError(t, err)

	require.Equal(t, whiteList, whiteListFromCache)
}

func TestCnsPriceIdQuery(t *testing.T) {
	_, cancel, storeHandler, tDown := setup(t)
	defer tDown()
	defer cancel()

	whiteList, err := storeHandler.CNSPriceIdQuery()
	require.NoError(t, err)
	require.NotNil(t, whiteList)

	require.Equal(t, []string{"cosmos", "terra-luna"}, whiteList)
}

func TestPriceTokenAggregator(t *testing.T) {
	_, cancel, storeHandler, tDown := setup(t)
	defer tDown()
	defer cancel()

	tokens := []string{"ATOMUSDT", "LUNAUSDT"}
	stores := []string{store.BinanceStore, store.CoingeckoStore}

	for _, tk := range tokens {
		for i, s := range stores {
			err := storeHandler.Store.UpsertToken(s, tk, float64(10+i), time.Now().Unix())
			require.NoError(t, err)
		}
	}

	err := storeHandler.PriceTokenAggregator()
	require.NoError(t, err)

	prices, err := storeHandler.Store.GetTokenPriceAndSupplies(tokens)
	require.NoError(t, err)

	for i, p := range prices {
		require.Equal(t, tokens[i], p.Symbol)
		require.Equal(t, 10.5, p.Price)
	}
}

func TestPriceFiatAggregator(t *testing.T) {
	_, cancel, storeHandler, tDown := setup(t)
	defer tDown()
	defer cancel()

	fiats := []string{"USDCHF", "USDEUR", "USDKRW"}
	stores := []string{store.FixerStore}

	for _, tk := range fiats {
		for i, s := range stores {
			err := storeHandler.Store.UpsertToken(s, tk, float64(10+i), time.Now().Unix())
			require.NoError(t, err)
		}
	}

	err := storeHandler.PriceFiatAggregator()
	require.NoError(t, err)

	prices, err := storeHandler.Store.GetFiatPrices(fiats)
	require.NoError(t, err)
	require.NotNil(t, prices)

	for i, p := range prices {
		require.Equal(t, fiats[i], p.Symbol)
		require.Equal(t, float64(10), p.Price)
	}
}

func TestGetTokenPriceAndSupplies(t *testing.T) {
	_, cancel, storeHandler, tDown := setup(t)
	defer tDown()
	defer cancel()

	upsertedTokens, tokens, err := upsertTokens(storeHandler)
	require.NoError(t, err)

	require.Nil(t, storeHandler.Cache.TokenPriceAndSupplies)

	tokensFromStore, err := storeHandler.GetTokenPriceAndSupplies(tokens)
	require.NoError(t, err)

	require.Equal(t, upsertedTokens, tokensFromStore)

	require.NotNil(t, storeHandler.Cache.TokenPriceAndSupplies)

	tokensFromCache, err := storeHandler.GetTokenPriceAndSupplies(tokens)
	require.NoError(t, err)

	require.Equal(t, upsertedTokens, tokensFromCache)
}

func TestGetFiatPrices(t *testing.T) {
	_, cancel, storeHandler, tDown := setup(t)
	defer tDown()
	defer cancel()

	require.Nil(t, storeHandler.Cache.FiatPrices)

	upsertedFiats, fiats, err := upsertFiats(storeHandler)
	require.NoError(t, err)

	fiatsFromStore, err := storeHandler.GetFiatPrices(fiats)
	require.NoError(t, err)

	require.Equal(t, upsertedFiats, fiatsFromStore)

	require.NotNil(t, storeHandler.Cache.FiatPrices)

	fiatsFromCache, err := storeHandler.GetFiatPrices(fiats)
	require.NoError(t, err)

	require.Equal(t, upsertedFiats, fiatsFromCache)
}

func getStoreHandler(t *testing.T, ts testserver.TestServer, logger *zap.SugaredLogger, cfg *config.Config) (*store.Handler, error) {
	t.Helper()
	db, err := sql.NewDB(ts.PGURL().String())
	if err != nil {
		return nil, err
	}

	storeHandler, err := store.NewStoreHandler(
		store.WithDB(db),
		store.WithLogger(logger),
		store.WithConfig(cfg),
		store.WithCache(nil),
	)
	if err != nil {
		return nil, err
	}

	return storeHandler, nil
}

func setup(t *testing.T) (context.Context, func(), *store.Handler, func()) {
	t.Helper()
	ts, err := testserver.NewTestServer()
	require.NoError(t, err)
	require.NoError(t, ts.WaitForInit())
	connStr := ts.PGURL().String()
	insertToken(t, connStr)

	cfg := &config.Config{ // config.Read() is not working. Fixing is not in scope of this task. That comes later.
		LogPath:               "",
		Debug:                 true,
		DatabaseConnectionURL: connStr,
		Interval:              "10s",
		WhitelistedFiats:      []string{"EUR", "KRW", "CHF"},
		RecoverCount:          3,
		WorkerPulse:           3 * time.Second,
	}

	logger := logging.New(logging.LoggingConfig{
		LogPath: cfg.LogPath,
		Debug:   cfg.Debug,
	})

	handler, err := getStoreHandler(t, ts, logger, cfg)
	require.NoError(t, err)
	require.NotNil(t, handler)

	ctx, cancel := context.WithCancel(context.Background())
	return ctx, cancel, handler, func() { ts.Stop() }
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

func upsertTokens(storeHandler *store.Handler) ([]types.TokenPriceAndSupply, []string, error) {
	// alphabetic order
	upsertTokens := []types.TokenPriceAndSupply{
		{
			Symbol: "ATOMUSDT",
			Price:  12.3,
			Supply: 456789,
		},
		{
			Symbol: "LUNAUSDT",
			Price:  98.7,
			Supply: 654321,
		},
	}

	var tokens []string
	for _, token := range upsertTokens {
		if err := storeHandler.Store.UpsertPrice(store.TokensStore, token.Price, token.Symbol); err != nil {
			return nil, nil, err
		}

		if err := storeHandler.Store.UpsertTokenSupply(store.CoingeckoSupplyStore, token.Symbol, token.Supply); err != nil {
			return nil, nil, err
		}

		tokens = append(tokens, token.Symbol)
	}
	return upsertTokens, tokens, nil
}

func upsertFiats(storeHandler *store.Handler) ([]types.FiatPrice, []string, error) {
	// alphabetic order
	upsertFiats := []types.FiatPrice{
		{
			Symbol: "CHFUSD",
			Price:  0.6,
		},
		{
			Symbol: "EURUSD",
			Price:  1.2,
		},
	}

	var fiats []string
	for _, fiat := range upsertFiats {
		if err := storeHandler.Store.UpsertPrice(store.FiatsStore, fiat.Price, fiat.Symbol); err != nil {
			return nil, nil, err
		}

		fiats = append(fiats, fiat.Symbol)
	}

	return upsertFiats, fiats, nil
}
