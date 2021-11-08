package daemon

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
	"math/rand"
	"time"

	"github.com/allinbits/emeris-price-oracle/price-oracle/config"
)

type (
	AggFunc    = func(context.Context, *sqlx.DB, *zap.SugaredLogger, *config.Config) error
	WorkerFunc = func(chan struct{}, time.Duration, *sqlx.DB, *zap.SugaredLogger, *config.Config, AggFunc) (chan interface{}, chan error)
)

func or(chans ...chan struct{}) chan struct{} {
	switch len(chans) {
	case 0:
		return nil
	case 1:
		return chans[0]
	}
	orDone := make(chan struct{})
	go func() {
		defer close(orDone)
		switch len(chans) {
		case 2:
			select {
			case <-chans[0]:
			case <-chans[1]:
			}
		default:
			select {
			case <-chans[0]:
			case <-chans[1]:
			case <-chans[2]:
			case <-or(append(chans[3:], orDone)...):
			}
		}
	}()
	return orDone
}

// MakeDaemon takes a WorkerFunc and wraps it with self-healing daemon-like functionality.
// When the worker is not responsive for a certain timeout period (timeout), it restarts
// the worker. It also has a recoverCount param, which indicates on how many times it
// will recover from errors in caused by the worker.
// recoverCount can have
//		1. Value 0, which means do not recover from fatal error.
//		2. Negative value, means always recover from fatal error.
// 		3. Positive value, self explaining.
//
// Right now heartbeat from the worker only indicates liveliness. In the future we plan to
// include meaningful data for monitoring/sampling.
func MakeDaemon(timeout time.Duration, recoverCount int, worker WorkerFunc) WorkerFunc {
	return func(
		done chan struct{},
		pulseInterval time.Duration,
		db *sqlx.DB,
		logger *zap.SugaredLogger,
		cfg *config.Config,
		fn AggFunc,
	) (chan interface{}, chan error) {
		heartbeat := make(chan interface{})
		errCh := make(chan error)
		go func() {
			defer close(heartbeat)
			defer close(errCh)

			var workerDone chan struct{}
			var workerHeartbeat <-chan interface{}
			var workerErr <-chan error

			startWorker := func() {
				workerDone = make(chan struct{})
				workerHeartbeat, workerErr = worker(or(workerDone, done), pulseInterval, db, logger, cfg, fn)
			}
			startWorker()

			// Info: daemon's pulse should be at least 2* the pulse of the worker.
			// So that worker does not compete with the daemon when trying to notify.
			// Add jitter so that all service does not request at once.
			seed := rand.NewSource(time.Now().UnixNano())
			randomInt := rand.New(seed).Int63n(1000)
			jitter := time.Duration(randomInt) * time.Millisecond
			pulse := time.Tick((2 * pulseInterval) + jitter)

		monitorLoop:
			for {
				timeoutSignal := time.After(timeout)
				for {
					select {
					case <-pulse:
						select {
						case heartbeat <- fmt.Sprintf("Daemon heartbeat"):
						default:
						}
					case beat := <-workerHeartbeat:
						// TODO: Send useful metric in future.
						logger.Infof("Heartbeat received: %v", beat)
						continue monitorLoop
					case err := <-workerErr:
						if recoverCount == 0 {
							return
						}
						// TODO: reduce recovery count only on irreversible errors.
						recoverCount--
						errCh <- err
						continue monitorLoop
					case <-timeoutSignal:
						logger.Errorf("Daemon: process unhealthy; restarting")
						close(workerDone)
						startWorker()
						continue monitorLoop
					case <-done:
						return
					}
				}
			}
		}()
		return heartbeat, errCh
	}
}
