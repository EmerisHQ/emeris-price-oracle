package database_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/allinbits/emeris-price-oracle/price-oracle/daemon"
	"github.com/stretchr/testify/require"

	"github.com/allinbits/emeris-price-oracle/price-oracle/database"
	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
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

	// alphabetic order; Check setupSubscription()
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
	prices, err := storeHandler.Store.GetTokens(types.SelectToken{Tokens: []string{"ATOMUSDT", "LUNAUSDT"}})
	require.NoError(t, err)

	for i, price := range prices {
		require.Equal(t, tokens[i].Symbol, price.Symbol)
		require.Equal(t, tokens[i].Price, price.Price)
	}

	go database.StartAggregate(ctx, storeHandler, logger, cfg, 3)

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
	_, storeHandler, cancel, logger, cfg, tDown := setupSubscription(t)
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
	_, storeHandler, cancel, logger, cfg, tDown := setupSubscription(t)
	defer tDown()
	defer cancel()

	//Check setupSubscription
	tokens := types.SelectToken{
		Tokens: []string{"ATOMUSDT", "LUNAUSDT"},
	}

	err := storeHandler.PricetokenAggregator(logger, cfg)
	require.NoError(t, err)

	prices, err := storeHandler.Store.GetTokens(tokens)
	require.NoError(t, err)

	for i, p := range prices {
		require.Equal(t, tokens.Tokens[i], p.Symbol)
		require.Equal(t, 11.5, p.Price)
	}
}

func TestPriceFiatAggregator(t *testing.T) {
	_, storeHandler, cancel, logger, cfg, tDown := setupSubscription(t)
	defer tDown()
	defer cancel()

	//Check setupSubscription
	fiats := types.SelectFiat{
		Fiats: []string{"USDCHF", "USDEUR", "USDKRW"},
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

func TestAveraging(t *testing.T) {
	nums := map[string]float64{
		"a": 1.1,
		"b": 2.2,
		"c": 3.3,
		"d": 4.4,
		"e": 5.5,
	}
	avg, err := database.Averaging(nums)
	require.NoError(t, err)
	require.Equal(t, 3.3, avg)

	_, err = database.Averaging(nil)
	require.Error(t, err)
	require.Equal(t, fmt.Errorf("Aggregator.Averaging(): nil price list recieved"), err)

	_, err = database.Averaging(map[string]float64{})
	require.Error(t, err)
	require.Equal(t, fmt.Errorf("Aggregator.Averaging(): empty price list recieved"), err)
}
