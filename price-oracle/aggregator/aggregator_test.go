package aggregator_test

import (
	"github.com/allinbits/emeris-price-oracle/price-oracle/store"
	"os"
	"testing"
	"time"

	"github.com/allinbits/emeris-price-oracle/price-oracle/aggregator"
	"github.com/allinbits/emeris-price-oracle/price-oracle/daemon"
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
	ctx, storeHandler, cancel, logger, cfg, tDown := setupSubscription(t)
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
	stores := []string{store.BinanceStore, store.CoingeckoStore}
	for _, token := range tokens {
		err := storeHandler.Store.UpsertPrice(store.TokensStore, token.Price, token.Symbol, logger)
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

	go aggregator.StartAggregate(ctx, storeHandler, logger, cfg, 3)

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
	_, storeHandler, cancel, logger, cfg, tDown := setupSubscription(t)
	defer tDown()
	defer cancel()

	runAsDaemon := daemon.MakeDaemon(10*time.Second, 2, aggregator.AggregateManager)
	done := make(chan struct{})
	hbCh, errCh := runAsDaemon(done, 100*time.Millisecond, logger, cfg, storeHandler.PriceFiatAggregator)

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
	_, storeHandler, cancel, logger, cfg, tDown := setupSubscription(t)
	defer tDown()
	defer cancel()

	numRecover := 2
	runAsDaemon := daemon.MakeDaemon(10*time.Second, numRecover, aggregator.AggregateManager)
	done := make(chan struct{})
	hbCh, errCh := runAsDaemon(done, 6*time.Second, logger, cfg, storeHandler.PriceFiatAggregator)

	// Wait for the process to start
	<-hbCh
	// Close the DB
	err := storeHandler.Store.Close()
	require.NoError(t, err)
	// Collect 2 error logs
	for i := 0; i < numRecover; i++ {
		require.Contains(t, (<-errCh).Error(), "sql: aggregator is closed")
	}
	// Ensure everything is closed
	_, ok := <-errCh
	require.Equal(t, false, ok)
	close(done)
	_, ok = <-hbCh
	require.Equal(t, false, ok)
}
