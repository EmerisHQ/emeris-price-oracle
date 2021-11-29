package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	models "github.com/allinbits/demeris-backend-models/cns"
	cnsDB "github.com/allinbits/emeris-cns-server/cns/database"
	"github.com/allinbits/emeris-price-oracle/price-oracle/config"
	"github.com/allinbits/emeris-price-oracle/price-oracle/database"
	"github.com/allinbits/emeris-price-oracle/price-oracle/sql"
	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"github.com/allinbits/emeris-price-oracle/utils/logging"
	"github.com/allinbits/emeris-price-oracle/utils/store"
	"github.com/cockroachdb/cockroach-go/v2/testserver"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRest(t *testing.T) {
	router, _, _, tDown := setup(t)
	defer tDown()

	s := NewServer(router.s.sh, router.s.ri, router.s.l, router.s.c)
	ch := make(chan struct{})
	go func() {
		close(ch)
		err := s.Serve(router.s.c.ListenAddr)
		require.NoError(t, err)
	}()
	<-ch // Wait for the goroutine to start. Still hack!!
	wantData := types.AllPriceResponse{
		Fiats: []types.FiatPriceResponse{
			{Symbol: "USDCHF", Price: 10},
			{Symbol: "USDEUR", Price: 20},
			{Symbol: "USDKRW", Price: 5},
		},
		Tokens: []types.TokenPriceResponse{
			{Price: 10, Symbol: "ATOMUSDT", Supply: 113563929433.0},
			{Price: 10, Symbol: "LUNAUSDT", Supply: 113563929433.0},
		},
	}
	err := insertWantData(router, wantData, router.s.l)
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
		Tokens  types.SelectToken
		Status  int
		Message string
	}{
		"Token: Not whitelisted": {
			types.SelectToken{Tokens: []string{"DOTUSDT"}},
			http.StatusForbidden,
			"Not whitelisting asset",
		},
		"Token: No value": {
			types.SelectToken{Tokens: []string{}},
			http.StatusForbidden,
			"Not allow 0 asset",
		},
		"Token: Nil value": {
			types.SelectToken{Tokens: nil},
			http.StatusForbidden,
			"Not allow nil asset",
		},
		"Token: Exceeds limit": {
			types.SelectToken{Tokens: []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"}},
			http.StatusForbidden,
			"Not allow More than 10 asset",
		},
	}

	for tName, expected := range testSetToken {
		t.Run(tName, func(t *testing.T) {
			jsonBytes, err := json.Marshal(expected.Tokens)
			require.NoError(t, err)

			url := fmt.Sprintf("http://%s%s", router.s.c.ListenAddr, getselectTokensPricesRoute)
			resp, err = http.Post(url, "application/json", bytes.NewBuffer(jsonBytes))
			require.NoError(t, err)

			body, err = ioutil.ReadAll(resp.Body)
			require.NoError(t, err)

			err = resp.Body.Close()
			require.NoError(t, err)

			var gotPost struct {
				Data    []types.TokenPriceResponse `json:"data"`
				Status  int                        `json:"status"`
				Message string                     `json:"message"`
			}

			err = json.Unmarshal(body, &gotPost)
			require.NoError(t, err)
			require.Equal(t, expected.Status, gotPost.Status)
			require.Equal(t, expected.Message, gotPost.Message)
		})
	}

	var testSetFiat = map[string]struct {
		Fiat    types.SelectFiat
		Status  int
		Message string
	}{
		"Fiat: Not whitelisted": {
			types.SelectFiat{Fiats: []string{"USDBDT"}},
			http.StatusForbidden,
			"Not whitelisting asset",
		},
		"Fiat: No value": {
			types.SelectFiat{Fiats: []string{}},
			http.StatusForbidden,
			"Not allow 0 asset",
		},
		"Fiat: Nil value": {
			types.SelectFiat{Fiats: nil},
			http.StatusForbidden,
			"Not allow nil asset",
		},
		"Fiat: Exceeds limit": {
			types.SelectFiat{Fiats: []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"}},
			http.StatusForbidden,
			"Not allow More than 10 asset",
		},
	}

	for tName, expected := range testSetFiat {
		t.Run(tName, func(t *testing.T) {
			jsonBytes, err := json.Marshal(expected.Fiat)
			require.NoError(t, err)

			url := fmt.Sprintf("http://%s%s", router.s.c.ListenAddr, getselectFiatsPricesRoute)
			resp, err = http.Post(url, "application/json", bytes.NewBuffer(jsonBytes))
			require.NoError(t, err)

			body, err = ioutil.ReadAll(resp.Body)
			require.NoError(t, err)

			err = resp.Body.Close()
			require.NoError(t, err)

			var gotPost struct {
				Data    []types.FiatPriceResponse `json:"data"`
				Status  int                       `json:"status"`
				Message string                    `json:"message"`
			}

			err = json.Unmarshal(body, &gotPost)
			require.NoError(t, err)
			require.Equal(t, expected.Status, gotPost.Status)
			require.Equal(t, expected.Message, gotPost.Message)
		})
	}
}

func setup(t *testing.T) (router, *gin.Context, *httptest.ResponseRecorder, func()) {
	tServer, err := testserver.NewTestServer()
	require.NoError(t, err)

	require.NoError(t, tServer.WaitForInit())

	connStr := tServer.PGURL().String()
	require.NotNil(t, connStr)

	storeHandler, err := getStoreHandler(t, tServer)
	require.NoError(t, err)

	// migrations
	err = storeHandler.Store.Init()
	require.NoError(t, err)

	// Put dummy data in cns DB
	insertToken(t, connStr)

	// Setup redis
	minRedis, err := miniredis.Run()
	require.NoError(t, err)

	cfg := &config.Config{ // config.Read() is not working. Fixing is not in scope of this task. That comes later.
		LogPath:               "",
		Debug:                 true,
		DatabaseConnectionURL: connStr,
		Interval:              "10s",
		Whitelistfiats:        []string{"EUR", "KRW", "CHF"},
		ListenAddr:            "127.0.0.1:9898",
		RedisExpiry:           10 * time.Second,
	}

	logger := logging.New(logging.LoggingConfig{
		LogPath: cfg.LogPath,
		Debug:   cfg.Debug,
	})

	w := httptest.NewRecorder()
	ctx, engine := gin.CreateTestContext(w)

	str, err := store.NewClient(minRedis.Addr())
	require.NoError(t, err)

	server := &Server{
		l:  logger,
		sh: storeHandler,
		c:  cfg,
		g:  engine,
		ri: str,
	}

	return router{s: server}, ctx, w, func() { tServer.Stop(); minRedis.Close() }
}

func getdb(t *testing.T, ts testserver.TestServer) (*sql.SqlDB, error) {
	connStr := ts.PGURL().String()
	return sql.NewDB(connStr)
}

func getStoreHandler(t *testing.T, ts testserver.TestServer) (*database.StoreHandler, error) {
	db, err := getdb(t, ts)
	if err != nil {
		return nil, err
	}

	storeHandler, err := database.NewStoreHandler(db)
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

func insertWantData(r router, wantData types.AllPriceResponse, l *zap.SugaredLogger) error {
	for _, f := range wantData.Fiats {

		if err := r.s.sh.Store.UpsertPrice(database.FiatsStore, f.Price, f.Symbol, l); err != nil {
			return err
		}
	}

	for _, t := range wantData.Tokens {

		if err := r.s.sh.Store.UpsertPrice(database.TokensStore, t.Price, t.Symbol, l); err != nil {
			return err
		}

		if err := r.s.sh.Store.UpsertTokenSupply(database.CoingeckoSupplyStore, t.Symbol, t.Supply, l); err != nil {
			return err
		}
	}

	return nil
}
