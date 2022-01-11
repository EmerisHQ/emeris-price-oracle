package priceprovider_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	models "github.com/allinbits/demeris-backend-models/cns"
	cnsDB "github.com/allinbits/emeris-cns-server/cns/database"
	"github.com/allinbits/emeris-price-oracle/price-oracle/sql"
	"github.com/allinbits/emeris-price-oracle/price-oracle/store"

	"github.com/allinbits/emeris-price-oracle/price-oracle/config"
	"github.com/allinbits/emeris-price-oracle/price-oracle/priceprovider"
	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"github.com/cockroachdb/cockroach-go/v2/testserver"
	"github.com/stretchr/testify/require"
	geckoTypes "github.com/superoo7/go-gecko/v3/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestStartSubscription(t *testing.T) {
	ctx, cancel, storeHandler, observedLogs, tDown := setupSubscription(t)
	defer tDown()
	defer cancel()

	go priceprovider.StartSubscription(ctx, storeHandler)

	// Validate Worker start message...
	require.Eventually(t, func() bool {
		count := 0
		for _, info := range observedLogs.All() {
			if strings.Contains(fmt.Sprintf("%s", info.ContextMap()["SubscriptionWorker"]), "Start") {
				count++
			}
		}
		return count == 3
	}, 25*time.Second, 2*time.Second)
}

func TestSubscriptionBinance(t *testing.T) {
	binance := types.Binance{
		Symbol: "ATOMUSDT",
		Price:  "-50.0", // A value that is never possible in real world.
	}

	b, err := json.Marshal(binance)
	require.NoError(t, err)

	_, cancel, storeHandler, _, tDown := setupSubscription(t)
	defer tDown()
	defer cancel()

	client := newTestClient(func(req *http.Request) *http.Response {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       ioutil.NopCloser(bytes.NewReader(b)),
		}
	})

	api := priceprovider.Api{
		Client:       client,
		StoreHandler: storeHandler,
	}

	err = api.SubscriptionBinance()
	require.NoError(t, err)

	prices, err := storeHandler.Store.GetPrices(store.BinanceStore)
	require.NoError(t, err)
	require.Equal(t, prices[0].Symbol, "ATOMUSDT")
	require.Equal(t, prices[0].Price, -50.0)
}

func TestSubscriptionCoingecko(t *testing.T) {
	_, cancel, storeHandler, _, tDown := setupSubscription(t)
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

	api := priceprovider.Api{
		Client:       client,
		StoreHandler: storeHandler,
	}

	err = api.SubscriptionCoingecko()
	require.NoError(t, err)

	prices, err := storeHandler.Store.GetPrices(store.CoingeckoStore)
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

	_, cancel, storeHandler, _, tDown := setupSubscription(t)
	defer tDown()
	defer cancel()

	client := newTestClient(func(req *http.Request) *http.Response {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       ioutil.NopCloser(bytes.NewReader(b)),
		}
	})

	api := priceprovider.Api{
		Client:       client,
		StoreHandler: storeHandler,
	}

	err = api.SubscriptionFixer()
	require.NoError(t, err)

	prices, err := storeHandler.Store.GetPrices(store.FixerStore)
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

func setupSubscription(t *testing.T) (context.Context, func(), *store.Handler, *observer.ObservedLogs, func()) {
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
		Interval:              "10s",
		WhitelistedFiats:      []string{"EUR", "KRW", "CHF"},
		HttpClientTimeout:     2 * time.Second,
	}

	observedZapCore, observedLogs := observer.New(zap.InfoLevel)
	observedLogger := zap.New(observedZapCore)

	insertToken(t, connStr)
	ctx, cancel := context.WithCancel(context.Background())

	storeHandler, err := getStoreHandler(t, testServer, observedLogger.Sugar(), cfg)
	require.NoError(t, err)
	require.NotNil(t, storeHandler.Store)

	return ctx, cancel, storeHandler, observedLogs, func() { testServer.Stop() }
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

func getDB(t *testing.T, ts testserver.TestServer) (*sql.SqlDB, error) {
	t.Helper()
	connStr := ts.PGURL().String()
	return sql.NewDB(connStr)
}

func getStoreHandler(t *testing.T, ts testserver.TestServer, logger *zap.SugaredLogger, cfg *config.Config) (*store.Handler, error) {
	t.Helper()
	db, err := getDB(t, ts)
	if err != nil {
		return nil, err
	}

	var spMu, chMu sync.RWMutex
	storeHandler, err := store.NewStoreHandler(
		store.WithDB(db),
		store.WithLogger(logger),
		store.WithConfig(cfg),
		store.WithSpotPriceCache(nil, &spMu),
		store.WithChartDataCache(nil, time.Second*1, &chMu),
	)
	if err != nil {
		return nil, err
	}

	return storeHandler, nil
}
