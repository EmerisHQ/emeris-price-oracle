package daemon

import (
	"errors"
	"fmt"
	"math/rand"
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

// or takes an arbitrary number of channels (chans) and return a channel
// named orDone. If any of the channels in (chans) is closed, orDone
// is also closed.
//
// If we have 4 channels named ch1...ch4, the logic is,
// if closed(ch1) || closed(ch2) || closed(ch3) || closed(ch4) -> return True.
// And thus the name `or`
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
// will numRecover from errors in caused by the worker.
// recoverCount can have
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
				logger.Infof("Daemon: starts function %v", GetFunctionName(fn))
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
			randomInt := rand.New(rand.NewSource(time.Now().UnixNano())).Int63n(10) + 10
			jitter := pulseInterval / time.Duration(randomInt)
			pulse := time.Tick((2 * pulseInterval) + jitter)

		monitorLoop:
			for {
				timeoutSignal := time.After(timeout)
				for {
					select {
					case <-pulse:
						select {
						case heartbeat <- fmt.Sprintf("Heartbeat from daemon"):
						default:
						}
					case beat := <-workerHeartbeat:
						// TODO: Send useful metric in future. Or metrics should be handled by
						// the worker? Revisit here later when implement monitoring for price-oracle.
						//
						// Until we figure what to do with the heartbeat, we just log it.
						logger.Infof("Daemon: heartbeat received: %v", beat)
						continue monitorLoop
					case err := <-workerFatalErr:
						logger.Infof("Daemon: received fatal error from worker: %v", err)
						if recoverCount == 0 {
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
