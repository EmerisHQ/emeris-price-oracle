package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/emerishq/emeris-price-oracle/price-oracle/config"
	"github.com/emerishq/emeris-price-oracle/price-oracle/daemon"
	"go.uber.org/zap"
)

func StartAggregate(ctx context.Context, storeHandler *Handler) {
	fetchInterval, err := time.ParseDuration(storeHandler.Cfg.Interval)
	if err != nil {
		storeHandler.Logger.Fatal(err)
	}

	var wg sync.WaitGroup
	runAsDaemon := daemon.MakeDaemon(fetchInterval*3, storeHandler.Cfg.RecoverCount, AggregateManager)

	workers := map[string]struct {
		worker daemon.AggFunc
		doneCh chan struct{}
	}{
		"token": {worker: storeHandler.PriceTokenAggregator, doneCh: make(chan struct{})},
		"fiat":  {worker: storeHandler.PriceFiatAggregator, doneCh: make(chan struct{})},
	}
	for _, properties := range workers {
		wg.Add(1)
		heartbeatCh, errCh := runAsDaemon(ctx, properties.doneCh, storeHandler.Cfg.WorkerPulse, storeHandler.Logger, storeHandler.Cfg, properties.worker)
		go func(ctx context.Context, done chan struct{}, workerName string, lastLogTime time.Time) {
			defer close(done)
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case heartbeat := <-heartbeatCh:
					// Reduce number of logs on std out. Without this we have a very busy std out.
					if time.Since(lastLogTime) > 60*time.Second {
						storeHandler.Logger.Infof("Heartbeat received: %v: %v", workerName, heartbeat)
						lastLogTime = time.Now()
					}
				case err, ok := <-errCh:
					// errCh is closed. Daemon process returned.
					if !ok {
						return
					}
					storeHandler.Logger.Errorf("Error: %T : %v", workerName, err)
				}
			}
		}(ctx, properties.doneCh, daemon.GetFunctionName(properties.worker), time.Now().Add(-60*time.Second))
	}
	// TODO: Handle signal. Start/stop worker.
	wg.Wait()
}

func AggregateManager(
	ctx context.Context,
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
			logger.Errorw("AggregateManager", "Err:", err)
			errCh <- err
			return
		}
		ticker := time.NewTicker(fetchInterval)
		defer ticker.Stop()
		pulse := time.NewTicker(pulseInterval)
		defer pulse.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if err := fn(ctx); err != nil {
					logger.Errorw("AggregateManager", "Worker returned Err:", err)
					errCh <- err
				}
			case <-pulse.C:
				select {
				case heartbeatCh <- fmt.Sprintf("AggregateManager(%v)", daemon.GetFunctionName(fn)):
				default:
				}
			}
		}
	}()
	return heartbeatCh, errCh
}

func Averaging(prices map[string]float64) (float64, error) {
	if prices == nil {
		return 0, fmt.Errorf("Store.Averaging(): nil price list received")
	}
	if len(prices) == 0 {
		return 0, fmt.Errorf("Store.Averaging(): empty price list received")
	}
	var total float64
	for _, p := range prices {
		total += p
	}
	return total / float64(len(prices)), nil
}
