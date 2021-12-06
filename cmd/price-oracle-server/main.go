package main

import (
	"context"
	"github.com/allinbits/emeris-price-oracle/price-oracle/config"
	"github.com/allinbits/emeris-price-oracle/price-oracle/priceprovider"
	"github.com/allinbits/emeris-price-oracle/price-oracle/rest"
	"github.com/allinbits/emeris-price-oracle/price-oracle/sql"
	"github.com/allinbits/emeris-price-oracle/price-oracle/store"
	"github.com/allinbits/emeris-price-oracle/utils/logging"
	"os"
	"os/signal"
	"sync"
	"syscall"
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
		store.WithDB(db),
		store.WithConfig(cfg),
		store.WithLogger(logger),
		store.WithCache(nil),
	)
	if err != nil {
		logger.Fatal(err)
	}

	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg.Add(2)
	go func() {
		defer wg.Done()
		store.StartAggregate(ctx, storeHandler, 5)
	}()
	go func() {
		defer wg.Done()
		priceprovider.StartSubscription(ctx, storeHandler, logger, cfg)
	}()

	restServer := rest.NewServer(storeHandler, logger, cfg)
	go func() {
		if err := restServer.Serve(cfg.ListenAddr); err != nil {
			logger.Panicw("rest http server error", "error", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	cancel()
	wg.Wait()
	logger.Info("Shutting down server...")
}
