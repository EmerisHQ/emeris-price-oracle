package database

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"go.uber.org/zap"

	"github.com/allinbits/emeris-price-oracle/price-oracle/config"
	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
)

type (
	aggFunc    = func(context.Context, *sqlx.DB, *zap.SugaredLogger, *config.Config) error
	WorkerFunc = func(chan struct{}, time.Duration, *sqlx.DB, *zap.SugaredLogger, *config.Config, aggFunc) (chan interface{}, chan error)
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

func DaemonMaker(timeout time.Duration, recoverCount int, worker WorkerFunc) WorkerFunc {
	return func(
		done chan struct{},
		pulseInterval time.Duration,
		db *sqlx.DB,
		logger *zap.SugaredLogger,
		cfg *config.Config,
		fn aggFunc,
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
					case beat := <-workerHeartbeat: // TODO: Send useful metric in future.
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

func StartAggregate2(ctx context.Context, logger *zap.SugaredLogger, cfg *config.Config, maxRecover int) {
	fetchInterval, err := time.ParseDuration(cfg.Interval)
	if err != nil {
		logger.Fatal(err)
	}
	d, err := New(cfg.DatabaseConnectionURL)
	if err != nil {
		logger.Fatal(err)
	}
	defer func() { _ = d.d.Close() }()

	var wg sync.WaitGroup
	runAsDaemon := DaemonMaker(fetchInterval*3, maxRecover, AggregateManager)

	workers := map[string]struct {
		worker aggFunc
		doneCh chan struct{}
	}{
		"token": {worker: PricetokenAggregator, doneCh: make(chan struct{})},
		"fiat":  {worker: PricefiatAggregator, doneCh: make(chan struct{})},
	}
	for name, properties := range workers {
		fmt.Println(name, properties)
		wg.Add(1)
		heartbeatCh, errCh := runAsDaemon(properties.doneCh, 3*time.Second, d.d.DB, logger, cfg, properties.worker)
		go func(ctx context.Context, done chan struct{}, workerName string) {
			defer close(done)
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case heartbeat := <-heartbeatCh:
					logger.Infof("Heartbeat received: %v: %v", workerName, heartbeat)
				case err := <-errCh:
					logger.Debugf("Error: %T : %v", workerName, err)
				}
			}
		}(ctx, properties.doneCh, getFunctionName(properties.worker))
	}
	// TODO: Handle signal. Start/stop worker.
	wg.Wait()
}

func AggregateManager(
	done chan struct{},
	pulseInterval time.Duration,
	db *sqlx.DB,
	logger *zap.SugaredLogger,
	cfg *config.Config,
	fn aggFunc,
) (chan interface{}, chan error) {
	logger.Infow("INFO", "DB AggregateManager Starts: ", getFunctionName(fn))
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
				if err := fn(nil, db, logger, cfg); err != nil {
					logger.Errorw("DB", "Aggregate WORK err", err)
					errCh <- err
				}
			case <-pulse:
				select {
				case heartbeatCh <- fmt.Sprintf("AggregateManager(%v)", getFunctionName(fn)):
				default:
				}
			}
		}
	}()
	return heartbeatCh, errCh
}

func getFunctionName(i interface{}) string {
	fullName := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
	parts := strings.Split(fullName, "/")
	return parts[len(parts)-1]
}

func StartAggregate(ctx context.Context, logger *zap.SugaredLogger, cfg *config.Config) {

	d, err := New(cfg.DatabaseConnectionURL)
	if err != nil {
		logger.Fatal(err)
	}
	defer d.d.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		AggregateWokers(ctx, d.d.DB, logger, cfg, PricetokenAggregator)
	}()
	go func() {
		defer wg.Done()
		AggregateWokers(ctx, d.d.DB, logger, cfg, PricefiatAggregator)
	}()

	wg.Wait()
}

func AggregateWokers(ctx context.Context, db *sqlx.DB, logger *zap.SugaredLogger, cfg *config.Config, fn aggFunc) {
	logger.Infow("INFO", "DB", "Aggregate WORK Start")
	interval, err := time.ParseDuration(cfg.Interval)
	ticker := time.Tick(interval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker:
			if err := fn(ctx, db, logger, cfg); err != nil {
				logger.Errorw("DB", "Aggregate WORK err", err)
			}

			if err != nil {
				logger.Errorw("DB", "Aggregate WORK err", err)
				return
			}
		}
	}
}

func PricetokenAggregator(ctx context.Context, db *sqlx.DB, logger *zap.SugaredLogger, cfg *config.Config) error {
	symbolkv := make(map[string][]float64)
	var query []string
	binanceQuery := "SELECT * FROM oracle.binance"
	//coinmarketcapQuery := "SELECT * FROM oracle.coinmarketcap"
	coinmarketgeckoQuery := "SELECT * FROM oracle.coingecko"
	query = append(query, binanceQuery)
	query = append(query, coinmarketgeckoQuery)

	whitelist := make(map[string]struct{})
	cnswhitelist, err := CnsTokenQuery(db)
	if err != nil {
		return fmt.Errorf("CnsTokenQuery: %w", err)
	}
	for _, token := range cnswhitelist {
		basetoken := token + types.USDTBasecurrency
		whitelist[basetoken] = struct{}{}
	}

	for _, q := range query {
		Prices := PriceQuery(db, logger, q)
		for _, apitokenList := range Prices {
			if _, ok := whitelist[apitokenList.Symbol]; !ok {
				continue
			}
			now := time.Now()
			if apitokenList.UpdatedAt < now.Unix()-60 {
				continue
			}
			Pricelist := symbolkv[apitokenList.Symbol]
			Pricelist = append(Pricelist, apitokenList.Price)
			symbolkv[apitokenList.Symbol] = Pricelist
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

		median := total / float64(len(symbolkv[token]))
		tx := db.MustBegin()

		result := tx.MustExec("UPDATE oracle.tokens SET price = ($1) WHERE symbol = ($2)", median, token)
		updateresult, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("DB update: %w", err)
		}
		//If you perform an update without a token column, it does not respond as an error; it responds with zero.
		//So you have to insert a new one in the column.
		if updateresult == 0 {
			tx.MustExec("INSERT INTO oracle.tokens VALUES (($1),($2));", token, median)
		}

		err = tx.Commit()
		if err != nil {
			return fmt.Errorf("DB commit: %w", err)
		}
		logger.Infow("Insert to median Token Price", token, median)
	}
	return nil
}

func PricefiatAggregator(ctx context.Context, db *sqlx.DB, logger *zap.SugaredLogger, cfg *config.Config) error {
	symbolkv := make(map[string][]float64)
	var query []string
	fixerQuery := "SELECT * FROM oracle.fixer"

	query = append(query, fixerQuery)
	whitelist := make(map[string]struct{})
	for _, fiat := range cfg.Whitelistfiats {
		basefiat := types.USDBasecurrency + fiat
		whitelist[basefiat] = struct{}{}
	}

	for _, q := range query {
		Prices := PriceQuery(db, logger, q)
		for _, apifiatList := range Prices {
			if _, ok := whitelist[apifiatList.Symbol]; !ok {
				continue
			}
			now := time.Now()
			if apifiatList.UpdatedAt < now.Unix()-60 {
				continue
			}
			Pricelist := symbolkv[apifiatList.Symbol]
			Pricelist = append(Pricelist, apifiatList.Price)
			symbolkv[apifiatList.Symbol] = Pricelist
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
		median := total / float64(len(symbolkv[fiat]))

		tx := db.MustBegin()

		result := tx.MustExec("UPDATE oracle.fiats SET price = ($1) WHERE symbol = ($2)", median, fiat)
		updateresult, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("DB update: %w", err)
		}
		if updateresult == 0 {
			tx.MustExec("INSERT INTO oracle.fiats VALUES (($1),($2));", fiat, median)
		}
		err = tx.Commit()
		if err != nil {
			return fmt.Errorf("DB commit: %w", err)
		}
		logger.Infow("Insert to median Fiat Price", fiat, median)
	}
	return nil
}

func PriceQuery(db *sqlx.DB, logger *zap.SugaredLogger, Query string) []types.Prices {
	var symbols []types.Prices
	var symbol types.Prices
	rows, err := db.Queryx(Query)
	if err != nil {
		logger.Fatalw("Fatal", "DB", err.Error(), "Duration", time.Second)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.StructScan(&symbol)
		if err != nil {
			logger.Fatalw("Fatal", "DB", err.Error(), "Duration", time.Second)
		}
		symbols = append(symbols, symbol)
	}
	return symbols
}
