package database

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/allinbits/emeris-price-oracle/price-oracle/config"
	"github.com/allinbits/emeris-price-oracle/price-oracle/daemon"
	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"go.uber.org/zap"
)

func StartAggregate(storeHandler *StoreHandler, ctx context.Context, logger *zap.SugaredLogger, cfg *config.Config, maxRecover int) {
	fetchInterval, err := time.ParseDuration(cfg.Interval)
	if err != nil {
		logger.Fatal(err)
	}

	var wg sync.WaitGroup
	runAsDaemon := daemon.MakeDaemon(fetchInterval*3, maxRecover, AggregateManager)

	workers := map[string]struct {
		worker daemon.AggFunc
		doneCh chan struct{}
	}{
		"token": {worker: storeHandler.PricetokenAggregator, doneCh: make(chan struct{})},
		"fiat":  {worker: storeHandler.PricefiatAggregator, doneCh: make(chan struct{})},
	}
	for _, properties := range workers {
		wg.Add(1)
		// TODO: Hack!! Move pulse (3 * time.Second) on abstraction later.
		heartbeatCh, errCh := runAsDaemon(properties.doneCh, 3*time.Second, logger, cfg, properties.worker)
		go func(ctx context.Context, done chan struct{}, workerName string) {
			defer close(done)
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case heartbeat := <-heartbeatCh:
					logger.Infof("Heartbeat received: %v: %v", workerName, heartbeat)
				case err, ok := <-errCh:
					// errCh is closed. Daemon process returned.
					if !ok {
						return
					}
					logger.Errorf("Error: %T : %v", workerName, err)
				}
			}
		}(ctx, properties.doneCh, daemon.GetFunctionName(properties.worker))
	}
	// TODO: Handle signal. Start/stop worker.
	wg.Wait()
}

func AggregateManager(
	done chan struct{},
	pulseInterval time.Duration,
	logger *zap.SugaredLogger,
	cfg *config.Config,
	fn daemon.AggFunc,
) (chan interface{}, chan error) {
	heartbeatCh := make(chan interface{})
	errCh := make(chan error)
	go func() {
		defer close(heartbeatCh)
		defer close(errCh)
		fetchInterval, err := time.ParseDuration(cfg.Interval)
		if err != nil {
			logger.Errorw("DB", "Aggregate WORK err", err)
			errCh <- err
			return
		}
		ticker := time.Tick(fetchInterval)
		pulse := time.Tick(pulseInterval)
		for {
			select {
			case <-done:
				return
			case <-ticker:
				if err := fn(logger, cfg); err != nil {
					errCh <- err
				}
			case <-pulse:
				select {
				case heartbeatCh <- fmt.Sprintf("AggregateManager(%v)", daemon.GetFunctionName(fn)):
				default:
				}
			}
		}
	}()
	return heartbeatCh, errCh
}

func (storeHandler *StoreHandler) PricetokenAggregator(logger *zap.SugaredLogger, cfg *config.Config) error {
	symbolkv := make(map[string][]float64)
	stores := []string{BinanceStore, CoingeckoStore}

	whitelist := make(map[string]struct{})
	cnswhitelist, err := storeHandler.CnsTokenQuery()
	if err != nil {
		return fmt.Errorf("CnsTokenQuery: %w", err)
	}
	for _, token := range cnswhitelist {
		basetoken := token + types.USDTBasecurrency
		whitelist[basetoken] = struct{}{}
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
			if token.UpdatedAt < now.Unix()-60 {
				continue
			}
			pricelist := symbolkv[token.Symbol]
			pricelist = append(pricelist, token.Price)
			symbolkv[token.Symbol] = pricelist
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

func (storeHandler *StoreHandler) PricefiatAggregator(logger *zap.SugaredLogger, cfg *config.Config) error {
	symbolkv := make(map[string][]float64)
	stores := []string{FixerStore}

	whitelist := make(map[string]struct{})
	for _, fiat := range cfg.Whitelistfiats {
		basefiat := types.USDBasecurrency + fiat
		whitelist[basefiat] = struct{}{}
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
			pricelist := symbolkv[fiat.Symbol]
			pricelist = append(pricelist, fiat.Price)
			symbolkv[fiat.Symbol] = pricelist
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
