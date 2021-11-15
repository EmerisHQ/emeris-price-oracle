package database

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/allinbits/emeris-price-oracle/price-oracle/config"
	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"go.uber.org/zap"
)

func StartAggregate(ctx context.Context, storeHandler *StoreHandler, logger *zap.SugaredLogger, cfg *config.Config) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		AggregateWokers(ctx, storeHandler, logger, cfg, PricetokenAggregator)
	}()
	go func() {
		defer wg.Done()
		AggregateWokers(ctx, storeHandler, logger, cfg, PricefiatAggregator)
	}()

	wg.Wait()
}

func AggregateWokers(ctx context.Context, storeHandler *StoreHandler, logger *zap.SugaredLogger, cfg *config.Config, fn func(*StoreHandler, *config.Config, *zap.SugaredLogger) error) {
	logger.Infow("INFO", "Subscription", "Aggregate WORK Start")
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := fn(storeHandler, cfg, logger); err != nil {
			logger.Errorw("Subscription", "Aggregate WORK err", err)
		}

		interval, err := time.ParseDuration(cfg.Interval)
		if err != nil {
			logger.Errorw("Subscription", "Aggregate WORK err", err)
			return
		}
		time.Sleep(interval)
	}
}

func PricetokenAggregator(storeHandler *StoreHandler, cfg *config.Config, logger *zap.SugaredLogger) error {
	symbolkv := make(map[string][]float64)
	query := []string{BinanceStore, CoingeckoStore}

	whitelist := make(map[string]struct{})
	cnswhitelist, err := storeHandler.CnsTokenQuery()
	if err != nil {
		return fmt.Errorf("CnsTokenQuery: %w", err)
	}
	for _, token := range cnswhitelist {
		basetoken := token + types.USDTBasecurrency
		whitelist[basetoken] = struct{}{}
	}

	for _, q := range query {
		prices, err := storeHandler.Store.GetPrices(q)
		if err != nil {
			return fmt.Errorf("Store.GetPrices(%s): %w", q, err)
		}
		for _, apitokenList := range prices {
			if _, ok := whitelist[apitokenList.Symbol]; !ok {
				continue
			}
			now := time.Now()
			if apitokenList.UpdatedAt < now.Unix()-60 {
				continue
			}
			pricelist := symbolkv[apitokenList.Symbol]
			pricelist = append(pricelist, apitokenList.Price)
			symbolkv[apitokenList.Symbol] = pricelist
		}
	}

	for token := range whitelist {
		var total float64 = 0
		for _, value := range symbolkv[token] {
			total += value
		}
		if len(symbolkv[token]) == 0 {
			return nil
		}

		mean := total / float64(len(symbolkv[token]))
		err = storeHandler.Store.UpsertPrice(TokensStore, mean, token, logger)
		if err != nil {
			return fmt.Errorf("Store.UpsertTokenPrice(%f,%s): %w", mean, token, err)
		}
	}
	return nil
}

func PricefiatAggregator(storeHandler *StoreHandler, cfg *config.Config, logger *zap.SugaredLogger) error {
	symbolkv := make(map[string][]float64)
	query := []string{FixerStore}

	whitelist := make(map[string]struct{})
	for _, fiat := range cfg.Whitelistfiats {
		basefiat := types.USDBasecurrency + fiat
		whitelist[basefiat] = struct{}{}
	}

	for _, q := range query {
		prices, err := storeHandler.Store.GetPrices(q)
		if err != nil {
			return fmt.Errorf("Store.GetPrices(%s): %w", q, err)
		}
		for _, apifiatList := range prices {
			if _, ok := whitelist[apifiatList.Symbol]; !ok {
				continue
			}
			now := time.Now()
			if apifiatList.UpdatedAt < now.Unix()-60 {
				continue
			}
			pricelist := symbolkv[apifiatList.Symbol]
			pricelist = append(pricelist, apifiatList.Price)
			symbolkv[apifiatList.Symbol] = pricelist
		}
	}
	for fiat := range whitelist {
		var total float64 = 0
		for _, value := range symbolkv[fiat] {
			total += value
		}
		if len(symbolkv[fiat]) == 0 {
			return nil
		}
		mean := total / float64(len(symbolkv[fiat]))

		err := storeHandler.Store.UpsertPrice(FiatsStore, mean, fiat, logger)
		if err != nil {
			return fmt.Errorf("Store.UpsertFiatPrice(%f,%s): %w", mean, fiat, err)
		}

	}
	return nil
}
