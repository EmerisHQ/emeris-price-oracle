package store

import (
	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"go.uber.org/zap"
)

type Store interface {
	Init() error                                                                                      //runs migrations
	GetTokens(types.SelectToken) ([]types.TokenPriceResponse, error)                                  //fetches all tokens from db tokens
	GetFiats(types.SelectFiat) ([]types.FiatPriceResponse, error)                                     //fetches all fiat tokens from db fiats
	GetTokenNames() ([]string, error)                                                                 //fetches whilelist with token names
	GetPriceIDs() ([]string, error)                                                                   //fetches whilelist with price ids
	GetPrices(from string) ([]types.Prices, error)                                                    //fetches prices from db table ex: binance,coingecko,fixer,tokens
	UpsertPrice(to string, price float64, token string, logger *zap.SugaredLogger) error              //upsert token or fiat price in db ex: tokens, fiats
	UpsertToken(to string, symbol string, price float64, time int64, logger *zap.SugaredLogger) error //upsert token or fiat to db. "to" indicates db name ex: binance,coingecko,fixer,tokens
	UpsertTokenSupply(to string, symbol string, supply float64, logger *zap.SugaredLogger) error      //upsert token supply to db. "to" indicates db name ex: binance,coingecko,fixer,tokens
}
