package database_test

import (
	"os"
	"testing"
	"time"

	"github.com/allinbits/emeris-price-oracle/price-oracle/daemon"
	"github.com/allinbits/emeris-price-oracle/price-oracle/database"
	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestMain(m *testing.M) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	zap.ReplaceGlobals(logger)
	os.Exit(m.Run())
}

func TestStartAggregate(t *testing.T) {
	storeHandler, ctx, cancel, logger, cfg, tDown := setupSubscription(t)
	defer tDown()
	defer cancel()

	// alphabetic order
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

	prices, err := storeHandler.Store.GetTokens(types.SelectToken{Tokens: []string{"ATOMUSDT", "LUNAUSDT"}})
	require.NoError(t, err)

	for i, price := range prices {
		require.Equal(t, tokens[i].Symbol, price.Symbol)
		require.Equal(t, tokens[i].Price, price.Price)
	}

	go database.StartAggregate(storeHandler, ctx, logger, cfg, 3)

	// Validate data updated on DB ..
	require.Eventually(t, func() bool {
		prices, err := storeHandler.Store.GetTokens(types.SelectToken{Tokens: []string{"ATOMUSDT", "LUNAUSDT"}})
		require.NoError(t, err)

		atomPrice := prices[0].Price
		lunaPrice := prices[1].Price
		return atomPrice == 11.5 && lunaPrice == 11.5

	}, 25*time.Second, 2*time.Second)
}

func TestAggregateManager_closes(t *testing.T) {
	storeHandler, _, cancel, logger, cfg, tDown := setupSubscription(t)
	defer tDown()
	defer cancel()

	runAsDaemon := daemon.MakeDaemon(10*time.Second, 2, database.AggregateManager)
	done := make(chan struct{})
	hbCh, errCh := runAsDaemon(done, 100*time.Millisecond, logger, cfg, storeHandler.PricefiatAggregator)

	// Collect 5 heartbeats and then close
	for i := 0; i < 5; i++ {
		<-hbCh
	}
	close(done)
	_, ok := <-hbCh
	require.Equal(t, false, ok)
	_, ok = <-errCh
	require.Equal(t, false, ok)
}

func TestAggregateManager_worker_restarts(t *testing.T) {
	storeHandler, _, cancel, logger, cfg, tDown := setupSubscription(t)
	defer tDown()
	defer cancel()

	numRecover := 2
	runAsDaemon := daemon.MakeDaemon(10*time.Second, numRecover, database.AggregateManager)
	done := make(chan struct{})
	hbCh, errCh := runAsDaemon(done, 6*time.Second, logger, cfg, storeHandler.PricefiatAggregator)

	// Wait for the process to start
	<-hbCh
	// Close the DB
	err := storeHandler.Store.Close()
	require.NoError(t, err)
	// Collect 2 error logs
	for i := 0; i < numRecover; i++ {
		require.Contains(t, (<-errCh).Error(), "sql: database is closed")
	}
	// Ensure everything is closed
	_, ok := <-errCh
	require.Equal(t, false, ok)
	close(done)
	_, ok = <-hbCh
	require.Equal(t, false, ok)
}
func TestPriceTokenAggregator(t *testing.T) {
	storeHandler, _, cancel, logger, cfg, tDown := setupSubscription(t)
	defer tDown()
	defer cancel()

	tokens := types.SelectToken{
		Tokens: []string{"ATOMUSDT", "LUNAUSDT"},
	}
	stores := []string{database.BinanceStore, database.CoingeckoStore}

	for _, tk := range tokens.Tokens {
		for i, s := range stores {
			err := storeHandler.Store.UpsertToken(s, tk, float64(10+i), time.Now().Unix(), logger)
			require.NoError(t, err)
		}
	}

	err := storeHandler.PricetokenAggregator(logger, cfg)
	require.NoError(t, err)

	prices, err := storeHandler.Store.GetTokens(tokens)
	require.NoError(t, err)

	for i, p := range prices {
		require.Equal(t, tokens.Tokens[i], p.Symbol)
		require.Equal(t, 10.5, p.Price)
	}
}

func TestPriceFiatAggregator(t *testing.T) {
	storeHandler, _, cancel, logger, cfg, tDown := setupSubscription(t)
	defer tDown()
	defer cancel()

	fiats := types.SelectFiat{
		Fiats: []string{"USDCHF", "USDEUR", "USDKRW"},
	}
	stores := []string{database.FixerStore}

	for _, tk := range fiats.Fiats {
		for i, s := range stores {
			err := storeHandler.Store.UpsertToken(s, tk, float64(10+i), time.Now().Unix(), logger)
			require.NoError(t, err)
		}
	}

	err := storeHandler.PricefiatAggregator(logger, cfg)
	require.NoError(t, err)

	prices, err := storeHandler.Store.GetFiats(fiats)
	require.NoError(t, err)
	require.NotNil(t, prices)

	for i, p := range prices {
		require.Equal(t, fiats.Fiats[i], p.Symbol)
		require.Equal(t, float64(10), p.Price)
	}
}
