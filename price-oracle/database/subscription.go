package database

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/allinbits/emeris-price-oracle/price-oracle/config"
	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	gecko "github.com/superoo7/go-gecko/v3"
	geckoTypes "github.com/superoo7/go-gecko/v3/types"
	"go.uber.org/zap"
)

const (
	BinanceURL       = "https://api.binance.com/api/v3/ticker/price"
	CoinmarketcapURL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
	FixerURL         = "https://data.fixer.io/api/latest"
)

type Api struct {
	Client       *http.Client
	StoreHandler *StoreHandler
}

func StartSubscription(ctx context.Context, storeHandler *StoreHandler, logger *zap.SugaredLogger, cfg *config.Config) {

	api := Api{
		Client:       &http.Client{Timeout: 2 * time.Second},
		StoreHandler: storeHandler,
	}

	var wg sync.WaitGroup
	for _, s := range []func(context.Context, *zap.SugaredLogger, *config.Config) error{
		api.SubscriptionBinance,
		api.SubscriptionCoingecko,
		api.SubscriptionFixer,
	} {
		subscriber := s
		wg.Add(1)
		go func() {
			defer wg.Done()
			SubscriptionWorker(ctx, logger, cfg, subscriber)
		}()
	}

	wg.Wait()
}

func SubscriptionWorker(ctx context.Context, logger *zap.SugaredLogger, cfg *config.Config, fn func(context.Context, *zap.SugaredLogger, *config.Config) error) {
	logger.Infow("INFO", "Database", "SubscriptionWorker Start")
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := fn(ctx, logger, cfg); err != nil {
			logger.Errorw("Database", "SubscriptionWorker", err)
		}

		interval, err := time.ParseDuration(cfg.Interval)
		if err != nil {
			logger.Errorw("Database", "SubscriptionWorker", err)
			return
		}
		time.Sleep(interval)
	}
}

func (api *Api) SubscriptionBinance(ctx context.Context, logger *zap.SugaredLogger, cfg *config.Config) error {
	whitelisttokens, err := api.StoreHandler.CnsTokenQuery()
	if err != nil {
		return fmt.Errorf("SubscriptionBinance CnsTokenQuery: %w", err)
	}
	if len(whitelisttokens) == 0 {
		return fmt.Errorf("SubscriptionBinance CnsTokenQuery: The token does not exist")
	}
	for _, token := range whitelisttokens {
		tokensum := token + types.USDTBasecurrency

		req, err := http.NewRequest("GET", BinanceURL, nil)
		if err != nil {
			return fmt.Errorf("SubscriptionBinance fetch binance: %w", err)
		}
		q := url.Values{}
		q.Add("symbol", tokensum)
		req.Header.Set("Accepts", "application/json")
		req.URL.RawQuery = q.Encode()

		resp, err := api.Client.Do(req)
		if err != nil {
			return fmt.Errorf("SubscriptionBinance fetch binance: %w", err)
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err := resp.Body.Close(); err != nil {
			return err
		}

		if err != nil {
			return fmt.Errorf("SubscriptionBinance read body: %w", err)
		}
		if resp.StatusCode != http.StatusOK {
			if resp.StatusCode == 400 {
				logger.Infof("SubscriptionBinance: %s, Status: %s", body, resp.Status)
				continue
			}
			return fmt.Errorf("SubscriptionBinance: %s, Status: %s", body, resp.Status)
		}

		bp := types.Binance{}
		if err = json.Unmarshal(body, &bp); err != nil {
			return fmt.Errorf("SubscriptionBinance unmarshal body: %w", err)
		}

		strToFloat, err := strconv.ParseFloat(bp.Price, 64)
		if err != nil {
			return fmt.Errorf("SubscriptionBinance convert price to float: %w", err)
		}
		if strToFloat == float64(0) {
			continue
		}

		now := time.Now()
		if err = api.StoreHandler.Store.UpsertToken(BinanceStore, bp.Symbol, strToFloat, now.Unix(), logger); err != nil {
			return fmt.Errorf("SubscriptionBinance, Store.UpsertToken(%s,%s,%f): %w", BinanceStore, bp.Symbol, strToFloat, err)
		}
		time.Sleep(1 * time.Second)
	}
	return nil
}

func (api *Api) SubscriptionCoingecko(ctx context.Context, logger *zap.SugaredLogger, cfg *config.Config) error {
	whitelisttokens, err := api.StoreHandler.CnsPriceIdQuery()
	if err != nil {
		return fmt.Errorf("SubscriptionCoingecko CnsPriceIdQuery: %w", err)
	}
	if len(whitelisttokens) == 0 {
		return fmt.Errorf("SubscriptionCoingecko CnsPriceIdQuery: The token does not exist")
	}

	cg := gecko.NewClient(api.Client)
	vsCurrency := types.USDBasecurrency
	perPage := 1
	page := 1
	sparkline := false
	pcp := geckoTypes.PriceChangePercentageObject
	priceChangePercentage := []string{pcp.PCP1h}
	order := geckoTypes.OrderTypeObject.MarketCapDesc
	market, err := cg.CoinsMarket(vsCurrency, whitelisttokens, order, perPage, page, sparkline, priceChangePercentage)
	if err != nil {
		return fmt.Errorf("SubscriptionCoingecko Market Query: %w", err)
	}

	for _, token := range *market {
		tokensum := strings.ToUpper(token.Symbol) + types.USDTBasecurrency

		now := time.Now()

		if err = api.StoreHandler.Store.UpsertToken(CoingeckoStore, tokensum, token.CurrentPrice, now.Unix(), logger); err != nil {
			return fmt.Errorf("SubscriptionCoingecko, Store.UpsertToken(%s,%s,%f): %w", CoingeckoStore, tokensum, token.CurrentPrice, err)
		}

		if err = api.StoreHandler.Store.UpsertTokenSupply(CoingeckoSupplyStore, tokensum, token.CirculatingSupply, logger); err != nil {
			return fmt.Errorf("SubscriptionCoingecko, Store.UpsertTokenSupply(%s,%s,%f): %w", CoingeckoSupplyStore, tokensum, token.CirculatingSupply, err)
		}
		time.Sleep(1 * time.Second)
	}
	return nil
}

func (api *Api) SubscriptionFixer(ctx context.Context, logger *zap.SugaredLogger, cfg *config.Config) error {
	req, err := http.NewRequest("GET", FixerURL, nil)
	if err != nil {
		return fmt.Errorf("SubscriptionFixer fetch Fixer: %w", err)
	}
	q := url.Values{}
	q.Add("access_key", cfg.Fixerapikey)
	q.Add("base", types.USDBasecurrency)
	q.Add("symbols", strings.Join(cfg.Whitelistfiats, ","))

	req.URL.RawQuery = q.Encode()

	resp, err := api.Client.Do(req)
	if err != nil {
		return fmt.Errorf("SubscriptionFixer fetch Fixer: %w", err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err := resp.Body.Close(); err != nil {
		return err
	}

	if err != nil {
		return fmt.Errorf("SubscriptionFixer read body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("SubscriptionFixer: %s, Status: %s", body, resp.Status)
	}

	bp := types.Fixer{}
	if err = json.Unmarshal(body, &bp); err != nil {
		return fmt.Errorf("SubscriptionFixer unmarshal body: %w", err)
	}
	if !bp.Success {
		logger.Infow("SubscriptionFixer", "The status message of the query is fail(Maybe the apikey problem)", bp.Success)
		return nil
	}
	var data map[string]float64
	if err = json.Unmarshal(bp.Rates, &data); err != nil {
		return fmt.Errorf("SubscriptionFixer unmarshal body: %w", err)
	}

	for _, fiat := range cfg.Whitelistfiats {
		fiatsum := types.USDBasecurrency + fiat
		d, ok := data[fiat]
		if !ok {
			logger.Infow("SubscriptionFixer", "From the provider list of deliveries price for symbol not found", fiatsum)
			return nil
		}

		now := time.Now()
		if err = api.StoreHandler.Store.UpsertToken(FixerStore, fiatsum, d, now.Unix(), logger); err != nil {
			return fmt.Errorf("SubscriptionFixer, Store.UpsertToken(%s,%s,%f): %w", FixerStore, fiatsum, d, err)
		}
	}
	return nil
}
