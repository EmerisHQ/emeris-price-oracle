package database

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"go.uber.org/zap"

	"github.com/allinbits/emeris-price-oracle/price-oracle/config"
	"github.com/allinbits/emeris-price-oracle/price-oracle/daemon"
	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
)

func StartAggregate(ctx context.Context, logger *zap.SugaredLogger, cfg *config.Config, maxRecover int) {
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
	runAsDaemon := daemon.MakeDaemon(fetchInterval*3, maxRecover, AggregateManager)

	workers := map[string]struct {
		worker daemon.AggFunc
		doneCh chan struct{}
	}{
		"token": {worker: PricetokenAggregator, doneCh: make(chan struct{})},
		"fiat":  {worker: PricefiatAggregator, doneCh: make(chan struct{})},
	}
	for _, properties := range workers {
		wg.Add(1)
		// TODO: Hack!! Move pulse (3 * time.Second) on abstraction later.
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
	db *sqlx.DB,
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
				if err := fn(nil, db, logger, cfg); err != nil {
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
		Prices, err := PriceQuery(db, q)
		if err != nil {
			return err
		}
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
		Prices, err := PriceQuery(db, q)
		if err != nil {
			return err
		}
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

func PriceQuery(db *sqlx.DB, Query string) ([]types.Prices, error) {
	var symbols []types.Prices
	var symbol types.Prices
	rows, err := db.Queryx(Query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		err := rows.StructScan(&symbol)
		if err != nil {
			return nil, err
		}
		symbols = append(symbols, symbol)
	}
	return symbols, nil
}
