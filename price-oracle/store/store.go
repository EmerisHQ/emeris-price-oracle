package store

import (
	"fmt"
	"github.com/allinbits/emeris-price-oracle/price-oracle/config"
	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"go.uber.org/zap"

	"time"
)

type Store interface {
	Init() error
	Close() error                                                                                     //runs migrations
	GetTokens(types.SelectToken) ([]types.TokenPriceResponse, error)                                  //fetches all tokens from db tokens
	GetFiats(types.SelectFiat) ([]types.FiatPriceResponse, error)                                     //fetches all fiat tokens from db fiats
	GetTokenNames() ([]string, error)                                                                 //fetches whilelist with token names
	GetPriceIDs() ([]string, error)                                                                   //fetches whilelist with price ids
	GetPrices(from string) ([]types.Prices, error)                                                    //fetches prices from db table ex: binance,coingecko,fixer,tokens
	UpsertPrice(to string, price float64, token string, logger *zap.SugaredLogger) error              //upsert token or fiat price in db ex: tokens, fiats
	UpsertToken(to string, symbol string, price float64, time int64, logger *zap.SugaredLogger) error //upsert token or fiat to db. "to" indicates db name ex: binance,coingecko,fixer
	UpsertTokenSupply(to string, symbol string, supply float64, logger *zap.SugaredLogger) error      //upsert token supply to db. "to" indicates db name ex: binance,coingecko,fixer
}

const (
	BinanceStore         = "oracle.binance"
	CoingeckoStore       = "oracle.coingecko"
	FixerStore           = "oracle.fixer"
	TokensStore          = "oracle.tokens"
	FiatsStore           = "oracle.fiats"
	CoingeckoSupplyStore = "oracle.coingeckosupply"
)

type Handler struct {
	Store Store
}

func NewStoreHandler(store Store) (*Handler, error) {
	if store == nil {
		return nil, fmt.Errorf("new_store.go, NewStoreHandler : nil store passed")
	}

	if err := store.Init(); err != nil {
		return nil, err
	}

	return &Handler{Store: store}, nil
}

func (storeHandler *Handler) CnsTokenQuery() ([]string, error) {
	whitelists, err := storeHandler.Store.GetTokenNames()
	if err != nil {
		return nil, err
	}
	return whitelists, nil
}

func (storeHandler *Handler) CnsPriceIdQuery() ([]string, error) {
	whitelists, err := storeHandler.Store.GetPriceIDs()
	if err != nil {
		return nil, err
	}
	return whitelists, nil
}

func (storeHandler *Handler) PricetokenAggregator(logger *zap.SugaredLogger, cfg *config.Config) error {
	symbolKV := make(map[string][]float64)
	stores := []string{BinanceStore, CoingeckoStore}

	whitelist := make(map[string]struct{})
	cnsWhitelist, err := storeHandler.CnsTokenQuery()
	if err != nil {
		return fmt.Errorf("CnsTokenQuery: %w", err)
	}
	for _, token := range cnsWhitelist {
		baseToken := token + types.USDTBasecurrency
		whitelist[baseToken] = struct{}{}
	}

	for _, s := range stores {
		prices, err := storeHandler.Store.GetPrices(s)
		if err != nil {
			return fmt.Errorf("Store.GetPrices(%s): %w", s, err)
		}
		for _, token := range prices {
			if _, ok := whitelist[token.Symbol]; !ok {
				continue
			}
			now := time.Now()

			//do not update if it was already updated in the last minute
			if token.UpdatedAt < now.Unix()-60 {
				continue
			}
			symbolKV[token.Symbol] = append(symbolKV[token.Symbol], token.Price)
		}
	}

	for token := range whitelist {
		var total float64 = 0
		for _, value := range symbolKV[token] {
			total += value
		}
		if len(symbolKV[token]) == 0 {
			return nil
		}

		mean := total / float64(len(symbolKV[token]))

		if err = storeHandler.Store.UpsertPrice(TokensStore, mean, token, logger); err != nil {
			return fmt.Errorf("Store.UpsertTokenPrice(%f,%s): %w", mean, token, err)
		}
	}
	return nil
}

func (storeHandler *Handler) PricefiatAggregator(logger *zap.SugaredLogger, cfg *config.Config) error {
	symbolKV := make(map[string][]float64)
	stores := []string{FixerStore}

	whitelist := make(map[string]struct{})
	for _, fiat := range cfg.Whitelistfiats {
		baseFiat := types.USDBasecurrency + fiat
		whitelist[baseFiat] = struct{}{}
	}

	for _, s := range stores {
		prices, err := storeHandler.Store.GetPrices(s)
		if err != nil {
			return fmt.Errorf("Store.GetPrices(%s): %w", s, err)
		}
		for _, fiat := range prices {
			if _, ok := whitelist[fiat.Symbol]; !ok {
				continue
			}
			now := time.Now()
			if fiat.UpdatedAt < now.Unix()-60 {
				continue
			}
			symbolKV[fiat.Symbol] = append(symbolKV[fiat.Symbol], fiat.Price)
		}
	}
	for fiat := range whitelist {
		var total float64 = 0
		for _, value := range symbolKV[fiat] {
			total += value
		}
		if len(symbolKV[fiat]) == 0 {
			return nil
		}
		mean := total / float64(len(symbolKV[fiat]))

		if err := storeHandler.Store.UpsertPrice(FiatsStore, mean, fiat, logger); err != nil {
			return fmt.Errorf("Store.UpsertFiatPrice(%f,%s): %w", mean, fiat, err)
		}

	}
	return nil
}
