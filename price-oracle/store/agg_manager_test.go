package store_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/emerishq/emeris-price-oracle/price-oracle/store"

	"github.com/emerishq/emeris-price-oracle/price-oracle/daemon"
	"github.com/emerishq/emeris-price-oracle/price-oracle/types"
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
	ctx, storeHandler, _, tDown := setup(t)
	defer tDown()

	// alphabetic order
	tokens := []types.TokenPriceAndSupply{
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
		err := storeHandler.Store.UpsertPrice(ctx, store.TokensStore, token.Price, token.Symbol)
		require.NoError(t, err)
		for i, s := range stores {
			err := storeHandler.Store.UpsertToken(ctx, s, token.Symbol, token.Price+float64(i+1), time.Now().Unix())
			require.NoError(t, err)
		}
	}

	prices, err := storeHandler.Store.GetTokenPriceAndSupplies(ctx, []string{"ATOMUSDT", "LUNAUSDT"})
	require.NoError(t, err)

	for i, price := range prices {
		require.Equal(t, tokens[i].Symbol, price.Symbol)
		require.Equal(t, tokens[i].Price, price.Price)
	}

	go store.StartAggregate(ctx, storeHandler)

	// Validate data updated on DB ..
	require.Eventually(t, func() bool {
		prices, err := storeHandler.Store.GetTokenPriceAndSupplies(ctx, []string{"ATOMUSDT", "LUNAUSDT"})
		require.NoError(t, err)

		atomPrice := prices[0].Price
		lunaPrice := prices[1].Price
		return atomPrice == 11.5 && lunaPrice == 11.5

	}, 25*time.Second, 2*time.Second)
}

func TestAggregateManager_closes(t *testing.T) {
	ctx, storeHandler, _, tDown := setup(t)
	defer tDown()

	runAsDaemon := daemon.MakeDaemon(10*time.Second, 2, store.AggregateManager)
	done := make(chan struct{})
	hbCh, errCh := runAsDaemon(ctx, done, 100*time.Millisecond, storeHandler.Logger, storeHandler.Cfg, storeHandler.PriceFiatAggregator)

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
	ctx, storeHandler, _, tDown := setup(t)
	defer tDown()

	numRecover := 2
	runAsDaemon := daemon.MakeDaemon(10*time.Second, numRecover, store.AggregateManager)
	done := make(chan struct{})
	hbCh, errCh := runAsDaemon(ctx, done, 6*time.Second, storeHandler.Logger, storeHandler.Cfg, storeHandler.PriceFiatAggregator)

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

func TestAveraging(t *testing.T) {
	nums := map[string]float64{
		"a": 1.1,
		"b": 2.2,
		"c": 3.3,
		"d": 4.4,
		"e": 5.5,
	}
	avg, err := store.Averaging(nums)
	require.NoError(t, err)
	require.Equal(t, 3.3, avg)

	_, err = store.Averaging(nil)
	require.Error(t, err)
	require.Equal(t, fmt.Errorf("Store.Averaging(): nil price list received"), err)

	_, err = store.Averaging(map[string]float64{})
	require.Error(t, err)
	require.Equal(t, fmt.Errorf("Store.Averaging(): empty price list received"), err)
}
