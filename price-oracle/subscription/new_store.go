package subscription

import (
	"fmt"

	Store "github.com/allinbits/emeris-price-oracle/price-oracle/store"
	_ "github.com/lib/pq"
)

type DBHandler struct {
	DB Store.Store
}

func NewDBHandler(db Store.Store) (*DBHandler, error) {
	if db == nil {
		return nil, fmt.Errorf("nil dbInterface.DB")
	}

	err := db.Init()
	if err != nil {
		return nil, err
	}

	return &DBHandler{DB: db}, nil
}

func (dbHandler *DBHandler) NewCnsTokenQuery() ([]string, error) {
	Whitelists, err := dbHandler.DB.GetTokenNames()
	if err != nil {
		return nil, err
	}
	return Whitelists, nil
}

func (dbHandler *DBHandler) NewCnsPriceIdQuery() ([]string, error) {
	Whitelists, err := dbHandler.DB.GetPriceIDs()
	if err != nil {
		return nil, err
	}
	return Whitelists, nil
}
