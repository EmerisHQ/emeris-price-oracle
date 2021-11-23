package store

import (
	"testing"
	"time"

	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T, store Store) {
	t.Run("Upsert and Get Tokens", func(t *testing.T) {
		list := types.SelectToken{
			Tokens: []string{"ATOM"},
		}

		err := store.UpsertPrice(TokensStore, -100, "ATOM")
		require.NoError(t, err)

		tokens, err := store.GetTokens(list)
		require.NoError(t, err)
		require.Equal(t, list.Tokens[0], tokens[0].Symbol)
	})

	t.Run("Upsert and Get Fiats", func(t *testing.T) {
		list := types.SelectFiat{
			Fiats: []string{"EUR"},
		}

		err := store.UpsertPrice(FiatsStore, -100, "EUR")
		require.NoError(t, err)

		tokens, err := store.GetFiats(list)
		require.NoError(t, err)
		require.Equal(t, list.Fiats[0], tokens[0].Symbol)
	})

	// t.Run("Get whilelist tokens and price IDs", func(t *testing.T) {
	// 	tokenNames, err := store.GetTokenNames()
	// 	require.NoError(t, err)
	// 	require.Contains(t, tokenNames, "ATOM")

	// 	priceIds, err := store.GetPriceIDs()
	// 	require.NoError(t, err)
	// 	require.Contains(t, priceIds, "cosmos")
	// })

	t.Run("Upsert token and Get prices", func(t *testing.T) {
		now := time.Now()
		err := store.UpsertToken(BinanceStore, "Test", -10, now.Unix())
		require.NoError(t, err)

		prices, err := store.GetPrices(BinanceStore)
		require.NoError(t, err)
		require.Equal(t, float64(-10), prices[0].Price)
		require.Equal(t, "Test", prices[0].Symbol)
		require.Equal(t, now.Unix(), prices[0].UpdatedAt)
	})

	t.Run("Upsert token supply and Get Tokens", func(t *testing.T) {
		err := store.UpsertPrice(TokensStore, -100, "ATOM")
		require.NoError(t, err)

		err = store.UpsertTokenSupply(CoingeckoSupplyStore, "ATOM", -23425)
		require.NoError(t, err)

		list := types.SelectToken{
			Tokens: []string{"ATOM"},
		}
		prices, err := store.GetTokens(list)
		require.NoError(t, err)
		require.Equal(t, "ATOM", prices[0].Symbol)
		require.Equal(t, float64(-23425), prices[0].Supply)
	})
}
