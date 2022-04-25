package daemon_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/emerishq/emeris-price-oracle/price-oracle/config"
	"github.com/emerishq/emeris-price-oracle/price-oracle/daemon"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestMakeDaemon(t *testing.T) {
	t.Parallel()
	dummyWorker, dummyAgg, logger, _, cfg := setupTest(t)

	recoverCount := 2
	runAsDaemon := daemon.MakeDaemon(1*time.Second, recoverCount, dummyWorker)
	require.IsType(t, runAsDaemon, dummyWorker)

	done := make(chan struct{})
	hbCh, errCh := runAsDaemon(done, 300*time.Millisecond, logger, cfg, dummyAgg)
	errorList := make([]error, 0)
	go func() {
		defer close(done)
		for {
			select {
			case hb := <-hbCh:
				logger.Infof("Main Caller: found heartbeat: %v", hb)
			case err, ok := <-errCh:
				if !ok {
					return
				}
				logger.Infof("Main Caller: found error: %v", err)
				errorList = append(errorList, err)
			}
		}
	}()

	<-done
	require.Equal(t, len(errorList), recoverCount)
}

func TestDaemon_recoverCount(t *testing.T) {
	dummyWorker, dummyAgg, logger, _, cfg := setupTest(t)

	testCases := map[string]struct {
		ln             int
		numRecover     int
		timeout, pulse time.Duration
	}{
		"Recover from 3 errors": {ln: -1, numRecover: 3, timeout: 50 * time.Millisecond, pulse: 10 * time.Millisecond},
		"Always recover":        {ln: 10, numRecover: -1, timeout: 50 * time.Millisecond, pulse: 10 * time.Millisecond},
		"Never recover":         {ln: -1, numRecover: 0, timeout: 50 * time.Millisecond, pulse: 10 * time.Millisecond},
	}

	for name, expected := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			runAsDaemon := daemon.MakeDaemon(expected.timeout, expected.numRecover, dummyWorker)
			done := make(chan struct{})
			hbCh, errCh := runAsDaemon(done, expected.pulse, logger, cfg, dummyAgg)
			errorList := make([]error, 0)
			go func() {
				defer close(done)
				for {
					select {
					case <-hbCh:
					case err, ok := <-errCh:
						if !ok {
							return
						}
						errorList = append(errorList, err)

						// "Always Recover" case, we need a way to stop the execution.
						if expected.numRecover == -1 && len(errorList) == expected.ln {
							return
						}
					}
				}
			}()

			<-done
			if expected.ln < 0 {
				require.Equal(t, len(errorList), expected.numRecover)
			} else {
				require.Equal(t, len(errorList), expected.ln)
			}
		})
	}
}

func TestDaemon_restart_non_responsive_worker(t *testing.T) {
	t.Parallel()
	dummyWorker, dummyAgg, logger, _, cfg := setupTest(t)

	daemonTimeout := 500 * time.Millisecond
	workerPulse := 2 * daemonTimeout

	runAsDaemon := daemon.MakeDaemon(daemonTimeout, 0, dummyWorker)
	done := make(chan struct{})
	_, errCh := runAsDaemon(done, workerPulse, logger, cfg, dummyAgg)
	require.Equal(t, <-errCh, daemon.ErrWorkerRestarted)
	close(done)
}

func TestDaemon_worker_not_responding_restart_twice(t *testing.T) {
	t.Parallel()
	_, dummyAgg, logger, _, cfg := setupTest(t)

	dummyWorker := func(
		done chan struct{},
		pulseInterval time.Duration,
		logger *zap.SugaredLogger,
		cfg *config.Config,
		fn daemon.AggFunc,
	) (chan interface{}, chan error) {
		heartbeatCh := make(chan interface{})
		errCh := make(chan error)
		go func(plsCnt int) {
			defer close(errCh)
			defer close(heartbeatCh)

			pulse := time.Tick(pulseInterval)
			callFn := time.Tick(pulseInterval * 2)
		Loop:
			for {
				if plsCnt >= 2 {
					time.Sleep(pulseInterval)
					continue Loop
				}
				select {
				case <-done:
					return
				case <-pulse:
					heartbeatCh <- fmt.Sprintf("Heartbeat from dummyWorker %v", plsCnt)
					plsCnt++
				case <-callFn:
					// XXX: Use a separate go routine to call this function?
					if err := fn(); err != nil {
						errCh <- err
					}
				}
			}
		}(0)
		return heartbeatCh, errCh
	}
	runAsDaemon := daemon.MakeDaemon(500*time.Millisecond, -1, dummyWorker)

	done := make(chan struct{})
	hbCh, errCh := runAsDaemon(done, 200*time.Millisecond, logger, cfg, dummyAgg)

	heartBeats := make([]string, 0)
	restartCnt := 0
Loop:
	for {
		select {
		case hb, ok := <-hbCh:
			if !ok {
				break Loop
			}
			heartBeats = append(heartBeats, fmt.Sprint(hb))
		case err, ok := <-errCh:
			if !ok {
				break Loop
			}
			if errors.Is(err, daemon.ErrWorkerRestarted) {
				if restartCnt >= 2 {
					close(done)
				}
				restartCnt++
				require.Equal(t, len(heartBeats), 2)
				heartBeats = make([]string, 0)
			}
		}
	}
}

func setupTest(t *testing.T) (daemon.WorkerFunc, daemon.AggFunc, *zap.SugaredLogger, *observer.ObservedLogs, *config.Config) {
	t.Helper()

	cfg := &config.Config{ // config.Read() is not working. Fixing is not in scope of this task. That comes later.
		LogPath:          "",
		Debug:            true,
		Interval:         "10s",
		WhitelistedFiats: []string{"EUR", "KRW", "CHF"},
	}

	observedZapCore, logs := observer.New(zap.InfoLevel)
	logger := zap.New(observedZapCore).Sugar()

	dummyAgg := func() error {
		return fmt.Errorf("not implemented")
	}
	dummyWorker := func(
		done chan struct{},
		pulseInterval time.Duration,
		logger *zap.SugaredLogger,
		cfg *config.Config,
		fn daemon.AggFunc,
	) (chan interface{}, chan error) {
		heartbeatCh := make(chan interface{})
		errCh := make(chan error)
		go func() {
			defer close(errCh)
			defer close(heartbeatCh)

			pulse := time.Tick(pulseInterval)
			callFn := time.Tick(pulseInterval * 2)
			for {
				select {
				case <-done:
					return
				case <-pulse:
					heartbeatCh <- "Heartbeat from dummyWorker"
				case <-callFn:
					logger.Infof("Worker: calling fn")
					// XXX: Use a separate go routine to call this function?
					if err := fn(); err != nil {
						logger.Infof("Worker: found Error: %v", err)
						errCh <- err
					}
				}
			}
		}()
		return heartbeatCh, errCh
	}
	return dummyWorker, dummyAgg, logger, logs, cfg
}
