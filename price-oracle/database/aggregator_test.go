package database_test

import (
	"testing"
	"time"

	"github.com/allinbits/emeris-price-oracle/price-oracle/database"
	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"github.com/stretchr/testify/require"
)

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

	err := database.PricetokenAggregator(storeHandler, cfg, logger)
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

	err := database.PricefiatAggregator(storeHandler, cfg, logger)
	require.NoError(t, err)

	prices, err := storeHandler.Store.GetFiats(fiats)
	require.NoError(t, err)
	require.NotNil(t, prices)

	for i, p := range prices {
		require.Equal(t, fiats.Fiats[i], p.Symbol)
		require.Equal(t, float64(10), p.Price)
	}
}
