package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/allinbits/emeris-price-oracle/price-oracle/config"
	"github.com/allinbits/emeris-price-oracle/price-oracle/daemon"
	"go.uber.org/zap"
)

func StartAggregate(
	ctx context.Context,
	storeHandler *Handler,
	logger *zap.SugaredLogger,
	cfg *config.Config,
	maxRecover int) {
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
		"token": {worker: storeHandler.PriceTokenAggregator, doneCh: make(chan struct{})},
		"fiat":  {worker: storeHandler.PriceFiatAggregator, doneCh: make(chan struct{})},
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