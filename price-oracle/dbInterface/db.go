package dbinterface

import "github.com/allinbits/emeris-price-oracle/price-oracle/types"

type db interface {
	InitDB() error                                                         //runs migrations
	GetAllTokens() []types.TokenPriceResponse                              //fetches all tokens from db
	GetAllFiats() []types.FiatPriceResponse                                //fetches all fiat tokens from db
	GetTokenNames() ([]string, error)                                      //fetches whilelist with token names
	GetPriceIDs() ([]string, error)                                        //fetches whilelist with price ids
	GetPrices(from string) ([]types.Prices, error)                         //fetches prices from db table ex: binance,coingecko,fixer,tokens
	UpsertTokenPrice(price float64, token string) error                    //upsert token price in db
	UpsertFiatPrice(price float64, token string) error                     //upsert fiat price in db
	UpsertToken(to string, symbol string, price float64, time int64) error //upsert token or fiat to db. "to" indicates db name
	UpsertTokenSupply(to string, symbol string, supply float64) error      //upsert token supply to db. "to" indicates db name
}
