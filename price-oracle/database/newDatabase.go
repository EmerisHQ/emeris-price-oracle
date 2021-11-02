package database

import (
	"fmt"

	dbInterface "github.com/allinbits/emeris-price-oracle/price-oracle/dbInterface"
	_ "github.com/lib/pq"
)

type DBHandler struct {
	db dbInterface.DB
}

func NewDBHandler(db dbInterface.DB) (DBHandler, error) {
	if db == nil {
		return DBHandler{}, fmt.Errorf("nil dbInterface.DB")
	}

	err := db.InitDB()
	if err != nil {
		return DBHandler{db: nil}, err
	}

	return DBHandler{db: db}, nil
}

// func (dbHandler *DBHandler) newCnsTokenQuery() ([]string, error) {
// 	Whitelists, err := dbHandler.db.GetTokenNames()
// 	if err != nil {
// 		return nil, err
// 	}
// 	return Whitelists, nil
// }

// func (dbHandler *DBHandler) newCnsPriceIdQuery() ([]string, error) {
// 	Whitelists, err := dbHandler.db.GetPriceIDs()
// 	if err != nil {
// 		return nil, err
// 	}
// 	return Whitelists, nil
// }
