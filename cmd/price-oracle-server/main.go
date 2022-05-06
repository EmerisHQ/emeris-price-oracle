package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/emerishq/emeris-price-oracle/price-oracle/config"
	"github.com/emerishq/emeris-price-oracle/price-oracle/priceprovider"
	"github.com/emerishq/emeris-price-oracle/price-oracle/rest"
	"github.com/emerishq/emeris-price-oracle/price-oracle/sql"
	"github.com/emerishq/emeris-price-oracle/price-oracle/store"
	"github.com/emerishq/emeris-utils/logging"
	"github.com/getsentry/sentry-go"
)

var Version = "not specified"

func main() {
	cfg, err := config.Read()
	if err != nil {
		panic(err)
	}

	logger := logging.New(logging.LoggingConfig{
		LogPath: cfg.LogPath,
		Debug:   cfg.Debug,
	})

	logger.Infow("price-oracle-server", "version", Version)

	db, err := sql.NewDB(cfg.DatabaseConnectionURL)
	if err != nil {
		logger.Fatal(err)
	}

	storeHandler, err := store.NewStoreHandler(
		store.WithDB(context.Background(), db),
		store.WithConfig(cfg),
		store.WithLogger(logger),
		store.WithSpotPriceCache(nil),
		store.WithChartDataCache(nil, time.Minute*5),
	)
	if err != nil {
		logger.Fatal(err)
	}

	if err := sentry.Init(sentry.ClientOptions{
		Dsn:              cfg.SentryDSN,
		Release:          Version,
		SampleRate:       cfg.SentrySampleRate,
		TracesSampleRate: cfg.SentryTracesSampleRate,
		Environment:      cfg.SentryEnvironment,
		AttachStacktrace: true,
	}); err != nil {
		logger.Fatalf("Sentry initialization failed: %v\n", err)
	}
	sentry.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetLevel(sentry.LevelWarning)
	})
	// TODO: @tbruyelle chk
	defer func() {
		logger.Infow("sentry flush", "success within deadline", sentry.Flush(time.Second*3))
	}()

	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg.Add(2)
	go func() {
		defer wg.Done()
		store.StartAggregate(ctx, storeHandler)
	}()
	go func() {
		defer wg.Done()
		priceprovider.StartSubscription(ctx, storeHandler)
	}()

	restServer := rest.NewServer(storeHandler, logger, cfg)
	fatalErr := make(chan error)
	go func() {
		fatalErr <- restServer.Serve(cfg.ListenAddr)
	}()

	// TODO: @tbruyelle chk
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-quit:
		logger.Info("Shutting down server...")
		cancel()
		wg.Wait()
	case err := <-fatalErr:
		cancel()
		wg.Wait()
		logger.Panicw("rest http server error", "error", err)
	}
}
