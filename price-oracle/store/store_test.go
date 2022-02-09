package store_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap/zaptest/observer"

	gecko "github.com/superoo7/go-gecko/v3"
	geckoTypes "github.com/superoo7/go-gecko/v3/types"

	"github.com/allinbits/emeris-price-oracle/price-oracle/config"
	"github.com/allinbits/emeris-price-oracle/price-oracle/store"
	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"go.uber.org/zap"

	models "github.com/allinbits/demeris-backend-models/cns"
	cnsDB "github.com/allinbits/emeris-cns-server/cns/database"
	"github.com/allinbits/emeris-price-oracle/price-oracle/sql"
	"github.com/cockroachdb/cockroach-go/v2/testserver"
	"github.com/stretchr/testify/require"
)

func TestNewStoreHandler(t *testing.T) {
	t.Parallel()
	_, _, storeHandler, _, tDown := setup(t)
	defer tDown()
	require.NotNil(t, storeHandler)

	storeHandler.SpotCache.Mu.RLock()
	require.Nil(t, storeHandler.SpotCache.WhitelistedTickers)
	require.Nil(t, storeHandler.SpotCache.FiatPrices)
	require.Nil(t, storeHandler.SpotCache.TokenPriceAndSupplies)
	storeHandler.SpotCache.Mu.RUnlock()

	_, err := storeHandler.GetCNSWhitelistedTokens()
	require.NoError(t, err)

	storeHandler.SpotCache.Mu.RLock()
	require.NotNil(t, storeHandler.SpotCache.WhitelistedTickers)
	storeHandler.SpotCache.Mu.RUnlock()

	require.Eventually(t, func() bool {
		storeHandler.SpotCache.Mu.RLock()
		isNil := storeHandler.SpotCache.WhitelistedTickers == nil
		storeHandler.SpotCache.Mu.RUnlock()
		return isNil
	}, 10*time.Second, 1*time.Second)

	_, fiats, err := upsertFiats(storeHandler)
	require.NoError(t, err)

	_, err = storeHandler.GetFiatPrices(fiats)
	require.NoError(t, err)

	storeHandler.SpotCache.Mu.RLock()
	require.NotNil(t, storeHandler.SpotCache.FiatPrices)
	storeHandler.SpotCache.Mu.RUnlock()

	require.Eventually(t, func() bool {
		storeHandler.SpotCache.Mu.RLock()
		isNil := storeHandler.SpotCache.FiatPrices == nil
		storeHandler.SpotCache.Mu.RUnlock()
		return isNil
	}, 10*time.Second, 1*time.Second)

	_, tokens, err := upsertTokens(storeHandler)
	require.NoError(t, err)

	_, err = storeHandler.GetTokenPriceAndSupplies(tokens)
	require.NoError(t, err)

	storeHandler.SpotCache.Mu.RLock()
	require.NotNil(t, storeHandler.SpotCache.TokenPriceAndSupplies)
	storeHandler.SpotCache.Mu.RUnlock()

	require.Eventually(t, func() bool {
		storeHandler.SpotCache.Mu.RLock()
		isNil := storeHandler.SpotCache.TokenPriceAndSupplies == nil
		storeHandler.SpotCache.Mu.RUnlock()
		return isNil
	}, 10*time.Second, 1*time.Second)

}

func TestGetCNSWhitelistedTokens(t *testing.T) {
	t.Parallel()
	_, cancel, storeHandler, _, tDown := setup(t)
	defer tDown()
	defer cancel()

	whiteList := []string{"ATOM", "LUNA"}

	storeHandler.SpotCache.Mu.RLock()
	require.Nil(t, storeHandler.SpotCache.WhitelistedTickers)
	storeHandler.SpotCache.Mu.RUnlock()

	whiteListFromStore, err := storeHandler.GetCNSWhitelistedTokens()
	require.NoError(t, err)

	require.Equal(t, whiteList, whiteListFromStore)

	storeHandler.SpotCache.Mu.RLock()
	require.NotNil(t, storeHandler.SpotCache.WhitelistedTickers)
	storeHandler.SpotCache.Mu.RUnlock()

	whiteListFromCache, err := storeHandler.GetCNSWhitelistedTokens()
	require.NoError(t, err)

	require.Equal(t, whiteList, whiteListFromCache)
}

func TestCnsPriceIdQuery(t *testing.T) {
	t.Parallel()
	_, cancel, storeHandler, _, tDown := setup(t)
	defer tDown()
	defer cancel()

	whiteList, err := storeHandler.GetCNSPriceIdsToTicker()
	require.NoError(t, err)
	require.NotNil(t, whiteList)

	require.Equal(t, map[string]string{"cosmos": "atom", "terra-luna": "luna"}, whiteList)
}

func TestPriceTokenAggregator(t *testing.T) {
	t.Parallel()
	_, cancel, storeHandler, _, tDown := setup(t)
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
	t.Parallel()
	_, cancel, storeHandler, _, tDown := setup(t)
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
	t.Parallel()
	_, cancel, storeHandler, _, tDown := setup(t)
	defer tDown()
	defer cancel()

	upsertedTokens, tokens, err := upsertTokens(storeHandler)
	require.NoError(t, err)

	storeHandler.SpotCache.Mu.RLock()
	require.Nil(t, storeHandler.SpotCache.TokenPriceAndSupplies)
	storeHandler.SpotCache.Mu.RUnlock()

	tokensFromStore, err := storeHandler.GetTokenPriceAndSupplies(tokens)
	require.NoError(t, err)

	require.Equal(t, upsertedTokens, tokensFromStore)

	storeHandler.SpotCache.Mu.RLock()
	require.NotNil(t, storeHandler.SpotCache.TokenPriceAndSupplies)
	storeHandler.SpotCache.Mu.RUnlock()

	tokensFromCache, err := storeHandler.GetTokenPriceAndSupplies(tokens)
	require.NoError(t, err)

	require.Equal(t, upsertedTokens, tokensFromCache)
}

func TestGetFiatPrices(t *testing.T) {
	t.Parallel()
	_, cancel, storeHandler, _, tDown := setup(t)
	defer tDown()
	defer cancel()

	storeHandler.SpotCache.Mu.RLock()
	require.Nil(t, storeHandler.SpotCache.FiatPrices)
	storeHandler.SpotCache.Mu.RUnlock()

	upsertedFiats, fiats, err := upsertFiats(storeHandler)
	require.NoError(t, err)

	fiatsFromStore, err := storeHandler.GetFiatPrices(fiats)
	require.NoError(t, err)

	require.Equal(t, upsertedFiats, fiatsFromStore)

	storeHandler.SpotCache.Mu.RLock()
	require.NotNil(t, storeHandler.SpotCache.FiatPrices)
	storeHandler.SpotCache.Mu.RUnlock()

	fiatsFromCache, err := storeHandler.GetFiatPrices(fiats)
	require.NoError(t, err)

	require.Equal(t, upsertedFiats, fiatsFromCache)
}

type roundTripFunc func(req *http.Request) *http.Response

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req), nil
}

func newTestClient(fn roundTripFunc, timeout time.Duration) *http.Client {
	return &http.Client{
		Transport: fn,
		Timeout:   timeout,
	}
}

func TestGetChartData_CorrectDataReturned(t *testing.T) {
	t.Parallel()
	_, cancel, storeHandler, _, tDown := setup(t)
	defer tDown()
	defer cancel()

	storeHandler.ChartCache.Mu.RLock()
	require.NotNil(t, storeHandler.ChartCache.Data)
	storeHandler.ChartCache.Mu.RUnlock()

	nowUnix := float32(time.Now().Unix())

	// Whatever the increment, should reflect in the response.
	dataBTC := generateChartData(2, nowUnix, 0)    // Returned data should all have the same TStamp.
	dataATOM := generateChartData(2, nowUnix, 110) // increment 110 should also match with the returned data.

	client := newTestClient(func(req *http.Request) *http.Response {
		data := dataATOM
		if strings.Contains(req.URL.Path, "bitcoin") {
			data = dataBTC
		}
		b, err := json.Marshal(data)
		require.NoError(t, err)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       ioutil.NopCloser(bytes.NewReader(b)),
		}
	}, time.Second)
	geckoClient := gecko.NewClient(client)

	// Test: Proper data is returned.
	tests := []struct {
		coinId   string
		days     string
		currency string
		want     *geckoTypes.CoinsIDMarketChart
	}{
		{"bitcoin", "1", "usd", dataBTC},
		{"cosmos", "14", "usd", dataATOM},
	}

	for _, tt := range tests {
		t.Run(tt.coinId, func(t *testing.T) {
			t.Parallel()
			resp, err := storeHandler.GetChartData(tt.coinId, tt.days, tt.currency, geckoClient)
			require.NoError(t, err)
			require.Equal(t, tt.want, resp)
		})
	}
}

func TestGetChartData_CacheHit(t *testing.T) {
	t.Parallel()
	_, cancel, storeHandler, _, tDown := setup(t)
	defer tDown()
	defer cancel()

	storeHandler.ChartCache.Mu.RLock()
	require.NotNil(t, storeHandler.ChartCache.Data)
	storeHandler.ChartCache.Mu.RUnlock()

	nowUnix := float32(time.Now().Unix())
	var clientInvoked int

	dataBTC := generateChartData(2, nowUnix, 0)

	client := newTestClient(func(req *http.Request) *http.Response {
		clientInvoked++
		b, err := json.Marshal(dataBTC)
		require.NoError(t, err)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       ioutil.NopCloser(bytes.NewReader(b)),
		}
	}, time.Second)
	geckoClient := gecko.NewClient(client)

	// Test: SpotCache hit!
	clientInvoked = 0
	resp, err := storeHandler.GetChartData("bitcoin", "1", "usd", geckoClient)
	require.NoError(t, err)
	require.Equal(t, dataBTC, resp)
	require.Equal(t, 1, clientInvoked)

	// Call 20 times. Client should not be invoked.
	for i := 0; i < 20; i++ {
		_, _ = storeHandler.GetChartData("bitcoin", "1", "usd", geckoClient)
		require.Equal(t, 1, clientInvoked)
	}
}

func TestGetChartData_CacheEmptied(t *testing.T) {
	t.Parallel()
	_, cancel, storeHandler, _, tDown := setup(t)
	defer tDown()
	defer cancel()

	nowUnix := float32(time.Now().Unix())
	dataBTC := generateChartData(2, nowUnix, 0)

	// Test: SpotCache is set and emptied correctly.
	for _, tt := range []struct {
		name             string
		days             string
		cacheGranularity string
	}{
		{name: "1 day should have cached in 5M", days: "1", cacheGranularity: "5M"},
		{name: "14 days should have cached in 1H", days: "14", cacheGranularity: "1H"},
		{name: "90 days should have cached in 1H", days: "90", cacheGranularity: "1H"},
		{name: "more than 90 days should have cached in 1D", days: "180", cacheGranularity: "1D"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := newTestClient(func(req *http.Request) *http.Response {
				b, err := json.Marshal(dataBTC)
				require.NoError(t, err)
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       ioutil.NopCloser(bytes.NewReader(b)),
				}
			}, time.Second)
			geckoClient := gecko.NewClient(client)

			resp, err := storeHandler.GetChartData("bitcoin", tt.days, "usd", geckoClient)
			require.NoError(t, err)
			require.Equal(t, resp, dataBTC)
			storeHandler.ChartCache.Mu.RLock()
			require.Equal(t, storeHandler.ChartCache.Data[tt.cacheGranularity]["bitcoin-usd"], dataBTC)
			storeHandler.ChartCache.Mu.RUnlock()

			time.Sleep(time.Second * 2)

			// We can only ensure that after the refresh interval (1 sec for test setup), the 5M
			// cache is evicted. Others are dependent on os clock, thus hard to test.
			if tt.days == "1" {
				storeHandler.ChartCache.Mu.RLock()
				require.Nil(t, storeHandler.ChartCache.Data[tt.cacheGranularity])
				storeHandler.ChartCache.Mu.RUnlock()
			}
		})
	}
}

func TestGetChartData_FetchDataVSReturnData(t *testing.T) {
	t.Parallel()
	_, cancel, storeHandler, _, tDown := setup(t)
	defer tDown()
	defer cancel()

	nowUnix := float32(time.Now().Unix())
	daysInSevenYears := 7 * 365
	hoursInNinetyDays := 24 * 90
	numberOfFiveMinutesOneDay := 24 * (60 / 5)

	maxData := generateChartData(daysInSevenYears, nowUnix, 24*3600 /*seconds in one day*/)
	ninetyDayData := generateChartData(hoursInNinetyDays, nowUnix, 24*3600)
	oneDayData := generateChartData(numberOfFiveMinutesOneDay, nowUnix, 5*60)

	// Test: Fetched max per granularity from coinGecko, bet returned proper amount.
	for _, tt := range []struct {
		name              string
		inCacheDataCount  int
		returnedDataCount int
		cacheGranularity  string
		fetchedData       *geckoTypes.CoinsIDMarketChart
	}{
		{
			"max",
			daysInSevenYears,
			daysInSevenYears,
			store.GranularityDay,
			maxData,
		},
		{
			"1",
			numberOfFiveMinutesOneDay,
			numberOfFiveMinutesOneDay,
			store.GranularityMinute,
			oneDayData,
		},
		{
			"14",
			hoursInNinetyDays,
			24 * 14,
			store.GranularityHour,
			ninetyDayData,
		},
		{
			"30",
			hoursInNinetyDays,
			24 * 30,
			store.GranularityHour,
			ninetyDayData,
		},
		{
			"180",
			daysInSevenYears,
			1 * 180,
			store.GranularityDay,
			maxData,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := newTestClient(func(req *http.Request) *http.Response {
				data := tt.fetchedData
				b, err := json.Marshal(data)
				require.NoError(t, err)
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       ioutil.NopCloser(bytes.NewReader(b)),
				}
			}, time.Second)
			geckoClient := gecko.NewClient(client)
			resp, err := storeHandler.GetChartData("bitcoin", tt.name, "usd", geckoClient)
			require.NoError(t, err)
			storeHandler.ChartCache.Mu.RLock()
			pricesFromCache := *storeHandler.ChartCache.Data[tt.cacheGranularity]["bitcoin-usd"].Prices
			storeHandler.ChartCache.Mu.RUnlock()
			require.Equal(t, tt.returnedDataCount, len(*resp.Prices))
			require.Equal(t, tt.inCacheDataCount, len(pricesFromCache))

			// Check if the value returned are correct!
			var priceCreatedManually = make([]float32, 0, tt.inCacheDataCount)
			var priceGotFromCache = make([]float32, 0, tt.inCacheDataCount)
			for i := 0; i < tt.inCacheDataCount; i++ {
				priceCreatedManually = append(priceCreatedManually, float32(i))
				priceGotFromCache = append(priceGotFromCache, pricesFromCache[i][1])
			}
			require.Equal(t, priceCreatedManually, priceGotFromCache)

			var pricesReturned = make([]float32, 0, tt.returnedDataCount)
			for i := 0; i < tt.returnedDataCount; i++ {
				pricesReturned = append(pricesReturned, (*resp.Prices)[i][1])
			}
			require.Equal(t, priceCreatedManually[tt.inCacheDataCount-tt.returnedDataCount:], pricesReturned)
		})
	}
}

func TestHandler_GetGeckoIdForToken(t *testing.T) {
	t.Parallel()
	_, cancel, storeHandler, observedLogs, tDown := setup(t)
	defer tDown()
	defer cancel()

	tests := []struct {
		name       string
		expected   map[string]string
		tokenNames []string
		checkLog   bool
		wantLog    []string
	}{
		{
			name:       "Coins not found",
			expected:   map[string]string{"unrealistic_token_1": "", "unrealistic_token_2": ""},
			tokenNames: []string{"UNREALISTIC_TOKEN_1", "UNREALISTIC_TOKEN_2"},
			checkLog:   true,
			wantLog:    []string{"UNREALISTIC_TOKEN_1", "UNREALISTIC_TOKEN_2"},
		},
		{
			name:       "No names -> Returns all whitelisted",
			expected:   map[string]string{"atom": "cosmos", "luna": "terra-luna"},
			tokenNames: nil,
			checkLog:   false,
			wantLog:    nil,
		},
		{
			name:       "Returned id only for valid coins",
			expected:   map[string]string{"atom": "cosmos", "unrealistic_token_1": ""},
			tokenNames: []string{"UNREALISTIC_TOKEN_1", "atom"},
			checkLog:   false,
			wantLog:    nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//t.Parallel()
			got, err := storeHandler.GetGeckoIdForTokenNames(tt.tokenNames)
			require.NoError(t, err)
			require.Equal(t, tt.expected, got)
			if tt.checkLog {
				for i, oLog := range observedLogs.All() {
					if i > 1 {
						break
					}
					require.Contains(t, strings.ToLower(tt.wantLog[i]), oLog.ContextMap()["GeckoId not found for"])
				}
			}
		})
	}
}

func TestGetGeckoIdFromAPI(t *testing.T) {
	t.Parallel()
	_, cancel, storeHandler, _, tDown := setup(t)
	defer tDown()
	defer cancel()

	type args struct {
		client *http.Client
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]string
		wantErr bool
	}{
		{
			name: "Should Success",
			args: args{
				client: &http.Client{Timeout: storeHandler.Cfg.HttpClientTimeout},
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.GetGeckoIdFromAPI(tt.args.client)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetGeckoIdFromAPI() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			require.NotNil(t, got)
			require.Greater(t, len(got), 10)
		})
	}
}

func generateChartData(n int, tm float32, increment float32) *geckoTypes.CoinsIDMarketChart {
	return &geckoTypes.CoinsIDMarketChart{
		Prices:       generateChartItems(n, tm, increment),
		MarketCaps:   generateChartItems(n, tm, increment),
		TotalVolumes: generateChartItems(n, tm, increment),
	}
}

func generateChartItems(n int, timestamp float32, increment float32) *[]geckoTypes.ChartItem {
	ret := make([]geckoTypes.ChartItem, 0, n)
	for i := 0; i < n; i++ {
		ret = append(ret, geckoTypes.ChartItem{timestamp, float32(i)})
		timestamp += increment
	}
	return &ret
}

func getStoreHandler(t *testing.T, ts testserver.TestServer, logger *zap.SugaredLogger, cfg *config.Config) (*store.Handler, error) {
	t.Helper()
	db, err := sql.NewDB(ts.PGURL().String())
	if err != nil {
		return nil, err
	}

	storeHandler, err := store.NewStoreHandler(
		store.WithDB(db), // This call rums the migrations.
		store.WithLogger(logger),
		store.WithConfig(cfg),
		store.WithSpotPriceCache(nil),
		store.WithChartDataCache(nil, time.Second*1),
	)
	if err != nil {
		return nil, err
	}

	return storeHandler, nil
}

func setup(t *testing.T) (context.Context, func(), *store.Handler, *observer.ObservedLogs, func()) {
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

	observedZapCore, observedLogs := observer.New(zap.InfoLevel)
	observedLogger := zap.New(observedZapCore)

	handler, err := getStoreHandler(t, ts, observedLogger.Sugar(), cfg)
	require.NoError(t, err)
	require.NotNil(t, handler)

	ctx, cancel := context.WithCancel(context.Background())
	return ctx, cancel, handler, observedLogs, func() { ts.Stop() }
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
