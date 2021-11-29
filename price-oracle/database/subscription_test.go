package database_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/allinbits/emeris-price-oracle/price-oracle/config"
	"github.com/allinbits/emeris-price-oracle/price-oracle/database"
	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"github.com/allinbits/emeris-price-oracle/utils/logging"
	"github.com/cockroachdb/cockroach-go/v2/testserver"
	"github.com/stretchr/testify/require"
	geckoTypes "github.com/superoo7/go-gecko/v3/types"
	"go.uber.org/zap"
)

func TestSubscriptionBinance(t *testing.T) {
	binance := types.Binance{
		Symbol: "ATOMUSDT",
		Price:  "-50.0", // A value that is never possible in real world.
	}

	b, err := json.Marshal(binance)
	require.NoError(t, err)

	ctx, storeHandler, cancel, logger, cfg, tDown := setupSubscription(t)
	defer tDown()
	defer cancel()

	client := newTestClient(func(req *http.Request) *http.Response {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       ioutil.NopCloser(bytes.NewReader(b)),
		}
	})

	api := database.Api{
		Client:       client,
		StoreHandler: storeHandler,
	}

	err = api.SubscriptionBinance(ctx, logger, cfg)
	require.NoError(t, err)

	prices, err := storeHandler.Store.GetPrices(database.BinanceStore)
	require.NoError(t, err)
	require.Equal(t, prices[0].Symbol, "ATOMUSDT")
	require.Equal(t, prices[0].Price, -50.0)
}

func TestSubscriptionCoingecko(t *testing.T) {
	ctx, storeHandler, cancel, logger, cfg, tDown := setupSubscription(t)
	defer tDown()
	defer cancel()

	atom := geckoTypes.CoinsMarketItem{
		CirculatingSupply: -18884562.3966529,
		CurrentPrice:      -39.41,
	}
	atom.Symbol = "atom"

	coingecko := geckoTypes.CoinsMarket{
		atom,
	}

	b, err := json.Marshal(coingecko)
	require.NoError(t, err)

	client := newTestClient(func(req *http.Request) *http.Response {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       ioutil.NopCloser(bytes.NewReader(b)),
		}
	})

	api := database.Api{
		Client:       client,
		StoreHandler: storeHandler,
	}

	err = api.SubscriptionCoingecko(ctx, logger, cfg)
	require.NoError(t, err)

	prices, err := storeHandler.Store.GetPrices(database.CoingeckoStore)
	require.NoError(t, err)
	require.Equal(t, prices[0].Symbol, "ATOMUSDT")
	require.Equal(t, prices[0].Price, -39.41)
}

func TestSubscriptionFixer(t *testing.T) {
	fixer := types.Fixer{
		Success: true,
		Rates: []byte(`
		{
			"CHF": 0.933058,
			"EUR": 0.806942,
			"KRW": 0.719154
		}
	`),
	}

	b, err := json.Marshal(&fixer)
	require.NoError(t, err)

	ctx, storeHandler, cancel, logger, cfg, tDown := setupSubscription(t)
	defer tDown()
	defer cancel()

	client := newTestClient(func(req *http.Request) *http.Response {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       ioutil.NopCloser(bytes.NewReader(b)),
		}
	})

	api := database.Api{
		Client:       client,
		StoreHandler: storeHandler,
	}

	err = api.SubscriptionFixer(ctx, logger, cfg)
	require.NoError(t, err)

	prices, err := storeHandler.Store.GetPrices(database.FixerStore)
	require.NoError(t, err)
	require.Equal(t, prices[1].Symbol, "USDEUR")
	require.Equal(t, prices[1].Price, 0.806942)
}

type roundTripFunc func(req *http.Request) *http.Response

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req), nil
}

func newTestClient(fn roundTripFunc) *http.Client {
	return &http.Client{
		Transport: fn,
	}
}

func setupSubscription(t *testing.T) (context.Context, *database.StoreHandler, func(), *zap.SugaredLogger, *config.Config, func()) {
	t.Helper()
	testServer, err := testserver.NewTestServer()
	require.NoError(t, err)
	require.NoError(t, testServer.WaitForInit())

	connStr := testServer.PGURL().String()
	require.NotNil(t, connStr)

	cfg := &config.Config{ // config.Read() is not working. Fixing is not in scope of this task. That comes later.
		LogPath:               "",
		Debug:                 true,
		DatabaseConnectionURL: connStr,
		WorkerPulse:           3 * time.Second,
		RecoverCount:          5,
		Interval:              "10s",
		HttpClientTimeOut:     2 * time.Second,
		Whitelistfiats:        []string{"EUR", "KRW", "CHF"},
	}

	logger := logging.New(logging.LoggingConfig{
		LogPath: cfg.LogPath,
		Debug:   cfg.Debug,
	})

	insertToken(t, connStr)
	ctx, cancel := context.WithCancel(context.Background())

	storeHandler, err := getStoreHandler(t, testServer)
	require.NoError(t, err)
	require.NotNil(t, storeHandler.Store)

	tokens := []types.TokenPriceResponse{
		{
			Symbol: "ATOMUSDT",
			Price:  10,
		},
		{
			Symbol: "LUNAUSDT",
			Price:  10,
		},
	}
	stores := []string{database.BinanceStore, database.CoingeckoStore}
	for _, token := range tokens {
		err := storeHandler.Store.UpsertPrice(database.TokensStore, token.Price, token.Symbol, logger)
		require.NoError(t, err)
		for i, s := range stores {
			err := storeHandler.Store.UpsertToken(s, token.Symbol, token.Price+float64(i+1), time.Now().Unix(), logger)
			require.NoError(t, err)
		}
	}

	fiats := types.SelectFiat{
		Fiats: []string{"USDCHF", "USDEUR", "USDKRW"},
	}
	stores = []string{database.FixerStore}
	for _, tk := range fiats.Fiats {
		for i, s := range stores {
			err := storeHandler.Store.UpsertToken(s, tk, float64(10+i), time.Now().Unix(), logger)
			require.NoError(t, err)
		}
	}

	return ctx, storeHandler, cancel, logger, cfg, func() { testServer.Stop() }
}
