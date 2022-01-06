package daemon

import (
	"crypto/rand"
	"errors"
	"math/big"
	"reflect"
	"runtime"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/allinbits/emeris-price-oracle/price-oracle/config"
)

type (
	AggFunc    = func() error
	WorkerFunc = func(
		chan struct{},
		time.Duration,
		*zap.SugaredLogger,
		*config.Config,
		AggFunc) (chan interface{}, chan error)
)

// ErrWorkerRestarted is used to indicate a restarting process is taking place.
var ErrWorkerRestarted = errors.New("daemon: process not responsive; restarting")

// or takes an arbitrary number of channels (param:<chans>) and return a channel
// named orDone. If any of the channels in (param:<chans>) is closed, orDone
// is also closed.
//
// If we have 4 channels named ch1...ch4, the logic is,
// if closed(ch1) || closed(ch2) || closed(ch3) || closed(ch4) -> closed(orDOne)
// must hold true. And thus the name `or`
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

// MakeDaemon takes a WorkerFunc (param:<worker>) and wraps it with self-healing
// daemon-like functionality. When the worker is not responsive for a certain timeout
// period (param:<timeout>), it restarts the worker. It also has a (param:<recoverCount>)
// which indicates on how many times it will recover from errors caused by the worker.
//
// (param:<recoverCount>) can have one of 3 types of values.
//		1. Value 0, which means do not numRecover from fatal error.
//		2. Negative value, means always numRecover from fatal error.
// 		3. Positive value, self explaining.
//
// Right now heartbeat from the worker only indicates liveliness. In the future we plan to
// include meaningful data for monitoring/sampling.
//
// Info: daemon's pulse should be at least 2* the pulse of the worker.
// So that worker does not compete with the daemon when trying to notify.
func MakeDaemon(timeout time.Duration, recoverCount int, worker WorkerFunc) WorkerFunc {
	return func(
		done chan struct{},
		pulseInterval time.Duration,
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
			var workerFatalErr <-chan error

			startWorker := func() {
				logger.Infow("Daemon", "starts function:", GetFunctionName(fn))
				workerDone = make(chan struct{})
				workerHeartbeat, workerFatalErr = worker(or(workerDone, done), pulseInterval, logger, cfg, fn)
			}
			startWorker()

			// Info: daemon's pulse should be at least 2* the pulse of the worker.
			// So that worker does not compete with the daemon when trying to notify.
			// Add jitter so that daemon and the worker does not overlap.
			//
			// Jitter should be at least 1/20 of the pulseInterval and not more than
			// 1/10 th of the pulseInterval.
			randInt, err := rand.Int(rand.Reader, big.NewInt(10))
			if err != nil {
				errCh <- err
				return
			}

			jitter := pulseInterval / time.Duration(randInt.Int64()+10)
			pulse := time.NewTicker((2 * pulseInterval) + jitter)
			defer pulse.Stop()

		monitorLoop:
			for {
				timeoutSignal := time.After(timeout)
				for {
					select {
					case <-pulse.C:
						select {
						case heartbeat <- "Heartbeat from daemon":
						default:
						}
					case beat := <-workerHeartbeat:
						// TODO: Send useful metric in future. Or metrics should be handled by
						// the worker? Revisit here later when implement monitoring for price-oracle.
						//
						// Until we figure what to do with the heartbeat, we just log it.
						logger.Infow("Daemon", "heartbeat received:", beat)
						continue monitorLoop
					case err := <-workerFatalErr:
						logger.Infow("Daemon", "received fatal error from worker:", err)
						if recoverCount == 0 {
							logger.Errorw("Daemon", "Terminating", "Max recovery limit reached:")
							return
						}
						recoverCount--
						errCh <- err
						close(workerDone)
						startWorker()
						continue monitorLoop
					case <-timeoutSignal:
						errCh <- ErrWorkerRestarted
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

func GetFunctionName(i interface{}) string {
	fullName := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
	parts := strings.Split(fullName, "/")
	return parts[len(parts)-1]
}
