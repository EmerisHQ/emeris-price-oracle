package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	geckoTypes "github.com/superoo7/go-gecko/v3/types"

	"github.com/emerishq/emeris-price-oracle/price-oracle/store"

	"github.com/cockroachdb/cockroach-go/v2/testserver"
	models "github.com/emerishq/demeris-backend-models/cns"
	cnsDB "github.com/emerishq/emeris-cns-server/cns/database"
	"github.com/emerishq/emeris-price-oracle/price-oracle/config"
	"github.com/emerishq/emeris-price-oracle/price-oracle/sql"
	"github.com/emerishq/emeris-price-oracle/price-oracle/types"
	"github.com/emerishq/emeris-utils/logging"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRest(t *testing.T) {
	router, _, _, tDown := setup(t)
	defer tDown()

	s := NewServer(router.s.sh, router.s.l, router.s.c)
	ch := make(chan struct{})
	go func() {
		close(ch)
		err := s.Serve(router.s.c.ListenAddr)
		if err != nil {
			require.Contains(t, err.Error(), "address already in use")
		}
	}()
	<-ch // Wait for the goroutine to start. Still hack!!
	wantData := types.AllPriceResponse{
		Fiats: []types.FiatPrice{
			{Symbol: "USDCHF", Price: 10},
			{Symbol: "USDEUR", Price: 20},
			{Symbol: "USDKRW", Price: 5},
		},
		Tokens: []types.TokenPriceAndSupply{
			{Price: 10, Symbol: "ATOMUSDT", Supply: 113563929433.0},
			{Price: 10, Symbol: "LUNAUSDT", Supply: 113563929433.0},
		},
	}
	err := insertWantData(router, wantData)
	require.NoError(t, err)

	resp, err := http.Get(fmt.Sprintf("http://%s%s", router.s.c.ListenAddr, getAllPriceRoute))
	require.NoError(t, err)

	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)

	err = resp.Body.Close()
	require.NoError(t, err)

	var got struct {
		Data types.AllPriceResponse `json:"data"`
	}
	err = json.Unmarshal(body, &got)
	require.NoError(t, err)

	require.Equal(t, wantData, got.Data)

	var testSetToken = map[string]struct {
		Tokens types.Tokens
		Status int
		Error  error
	}{
		"Token: Not whitelisted": {
			types.Tokens{Tokens: []string{"DOTUSDT"}},
			http.StatusForbidden,
			errNotWhitelistedAsset,
		},
		"Token: No value": {
			types.Tokens{Tokens: []string{}},
			http.StatusForbidden,
			errZeroAsset,
		},
		"Token: Nil value": {
			types.Tokens{Tokens: nil},
			http.StatusForbidden,
			errNilAsset,
		},
		"Token: Exceeds limit": {
			types.Tokens{Tokens: []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"}},
			http.StatusForbidden,
			errAssetLimitExceed,
		},
	}

	for tName, expected := range testSetToken {
		t.Run(tName, func(t *testing.T) {
			t.Parallel()
			jsonBytes, err := json.Marshal(expected.Tokens)
			require.NoError(t, err)

			url := fmt.Sprintf("http://%s%s", router.s.c.ListenAddr, getTokensPricesRoute)
			resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonBytes))
			require.NoError(t, err)

			body, err := ioutil.ReadAll(resp.Body)
			require.NoError(t, err)

			err = resp.Body.Close()
			require.NoError(t, err)

			var restError restError
			err = json.Unmarshal(body, &restError)
			require.NoError(t, err)

			require.Equal(t, expected.Status, resp.StatusCode)
			require.Equal(t, restError.Error, expected.Error.Error())
		})
	}

	var testSetFiat = map[string]struct {
		Fiat   types.Fiats
		Status int
		Error  error
	}{
		"Fiat: Not whitelisted": {
			types.Fiats{Fiats: []string{"USDBDT"}},
			http.StatusForbidden,
			errNotWhitelistedAsset,
		},
		"Fiat: No value": {
			types.Fiats{Fiats: []string{}},
			http.StatusForbidden,
			errZeroAsset,
		},
		"Fiat: Nil value": {
			types.Fiats{Fiats: nil},
			http.StatusForbidden,
			errNilAsset,
		},
		"Fiat: Exceeds limit": {
			types.Fiats{Fiats: []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"}},
			http.StatusForbidden,
			errAssetLimitExceed,
		},
	}

	for tName, expected := range testSetFiat {
		t.Run(tName, func(t *testing.T) {
			t.Parallel()
			jsonBytes, err := json.Marshal(expected.Fiat)
			require.NoError(t, err)

			url := fmt.Sprintf("http://%s%s", router.s.c.ListenAddr, getFiatsPricesRoute)
			resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonBytes))
			require.NoError(t, err)

			body, err := ioutil.ReadAll(resp.Body)
			require.NoError(t, err)

			err = resp.Body.Close()
			require.NoError(t, err)

			var restError restError
			err = json.Unmarshal(body, &restError)
			require.NoError(t, err)

			require.Equal(t, expected.Status, resp.StatusCode)
			require.Equal(t, restError.Error, expected.Error.Error())
		})
	}
}

func setup(t *testing.T) (router, *gin.Context, *httptest.ResponseRecorder, func()) {
	tServer, err := testserver.NewTestServer()
	require.NoError(t, err)

	require.NoError(t, tServer.WaitForInit())

	connStr := tServer.PGURL().String()
	require.NotNil(t, connStr)

	fp, err := getFreePort()
	require.NoError(t, err)

	cfg := &config.Config{
		LogPath:               "",
		Debug:                 true,
		DatabaseConnectionURL: connStr,
		Interval:              "10s",
		WhitelistedFiats:      []string{"EUR", "KRW", "CHF"},
		ListenAddr:            fmt.Sprintf("127.0.0.1:%d", fp),
		MaxAssetsReq:          10,
		HttpClientTimeout:     1 * time.Second,
	}

	logger := logging.New(logging.LoggingConfig{
		LogPath: cfg.LogPath,
		Debug:   cfg.Debug,
	})

	// Cache 1 day chart data for bitcoin-eur combo. Used in route_chartData_test.go
	chartData := generateChartData(12*24, 0, 0)
	chartDataCache := store.ChartDataCache{
		Data:            map[string]map[string]*geckoTypes.CoinsIDMarketChart{"5M": {"bitcoin-eur": chartData}},
		Mu:              sync.RWMutex{},
		RefreshInterval: 3 * time.Minute,
	}
	storeHandler, err := getStoreHandler(t, tServer, logger, cfg, &chartDataCache)
	require.NoError(t, err)

	// Put dummy data in cns DB
	insertToken(t, connStr)

	w := httptest.NewRecorder()
	ctx, engine := gin.CreateTestContext(w)
	ctx.Request = httptest.NewRequest("", "/", nil)

	server := &Server{
		l:  logger,
		sh: storeHandler,
		c:  cfg,
		g:  engine,
	}

	return router{s: server}, ctx, w, func() { tServer.Stop() }
}

func getDB(t *testing.T, ts testserver.TestServer) (*sql.SqlDB, error) {
	t.Helper()
	connStr := ts.PGURL().String()
	return sql.NewDB(connStr)
}

func getStoreHandler(t *testing.T, ts testserver.TestServer, logger *zap.SugaredLogger, cfg *config.Config, chart *store.ChartDataCache) (*store.Handler, error) {
	db, err := getDB(t, ts)
	if err != nil {
		return nil, err
	}

	storeHandler, err := store.NewStoreHandler(
		store.WithDB(context.Background(), db),
		store.WithLogger(logger),
		store.WithConfig(cfg),
		store.WithSpotPriceCache(nil),
		store.WithChartDataCache(chart, chart.RefreshInterval /*not used in this case as chart is not nil*/),
	)
	if err != nil {
		return nil, err
	}

	return storeHandler, nil
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

func insertWantData(r router, wantData types.AllPriceResponse) error {
	for _, f := range wantData.Fiats {

		if err := r.s.sh.Store.UpsertPrice(context.Background(), store.FiatsStore, f.Price, f.Symbol); err != nil {
			return err
		}
	}

	for _, t := range wantData.Tokens {

		if err := r.s.sh.Store.UpsertPrice(context.Background(), store.TokensStore, t.Price, t.Symbol); err != nil {
			return err
		}

		if err := r.s.sh.Store.UpsertTokenSupply(context.Background(), store.CoingeckoSupplyStore, t.Symbol, t.Supply); err != nil {
			return err
		}
	}

	return nil
}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port, nil
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
