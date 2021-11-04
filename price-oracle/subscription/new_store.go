package subscription

import (
	"fmt"

	Store "github.com/allinbits/emeris-price-oracle/price-oracle/store"
	_ "github.com/lib/pq"
)

type StoreHandler struct {
	Store Store.Store
}

func NewStoreHandler(store Store.Store) (*StoreHandler, error) {
	if store == nil {
		return nil, fmt.Errorf("new_store.go, NewStoreHandler : nil store passed")
	}

	err := store.Init()
	if err != nil {
		return nil, err
	}

	return &StoreHandler{Store: store}, nil
}

func (storeHandler *StoreHandler) CnsTokenQuery() ([]string, error) {
	Whitelists, err := storeHandler.Store.GetTokenNames()
	if err != nil {
		return nil, err
	}
	return Whitelists, nil
}

func (storeHandler *StoreHandler) CnsPriceIdQuery() ([]string, error) {
	Whitelists, err := storeHandler.Store.GetPriceIDs()
	if err != nil {
		return nil, err
	}
	return Whitelists, nil
}
