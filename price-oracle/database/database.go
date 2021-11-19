package database

import (
	"fmt"

	"github.com/allinbits/emeris-price-oracle/price-oracle/store"
	_ "github.com/lib/pq"
)

const (
	BinanceStore         = "oracle.binance"
	CoingeckoStore       = "oracle.coingecko"
	FixerStore           = "oracle.fixer"
	TokensStore          = "oracle.tokens"
	FiatsStore           = "oracle.fiats"
	CoingeckoSupplyStore = "oracle.coingeckosupply"
)

type StoreHandler struct {
	Store store.Store
}

func NewStoreHandler(store store.Store) (*StoreHandler, error) {
	if store == nil {
		return nil, fmt.Errorf("new_store.go, NewStoreHandler : nil store passed")
	}

	if err := store.Init(); err != nil {
		return nil, err
	}

	return &StoreHandler{Store: store}, nil
}

func (storeHandler *StoreHandler) CnsTokenQuery() ([]string, error) {
	whitelists, err := storeHandler.Store.GetTokenNames()
	if err != nil {
		return nil, err
	}
	return whitelists, nil
}

func (storeHandler *StoreHandler) CnsPriceIdQuery() ([]string, error) {
	whitelists, err := storeHandler.Store.GetPriceIDs()
	if err != nil {
		return nil, err
	}
	return whitelists, nil
}
