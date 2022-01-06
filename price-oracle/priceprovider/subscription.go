package priceprovider

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

	"github.com/allinbits/emeris-price-oracle/price-oracle/daemon"
	"github.com/allinbits/emeris-price-oracle/price-oracle/store"

	"github.com/allinbits/emeris-price-oracle/price-oracle/config"
	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	gecko "github.com/superoo7/go-gecko/v3"
	geckoTypes "github.com/superoo7/go-gecko/v3/types"
	"go.uber.org/zap"
)

const (
	BinanceURL = "https://api.binance.com/api/v3/ticker/price"
	FixerURL   = "https://data.fixer.io/api/latest"

	// CoinMarketCapURL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
)

type Api struct {
	Client       *http.Client
	StoreHandler *store.Handler
}

func StartSubscription(ctx context.Context, storeHandler *store.Handler) {
	api := Api{
		Client:       &http.Client{Timeout: 0},
		StoreHandler: storeHandler,
	}

	var wg sync.WaitGroup
	for _, s := range []daemon.AggFunc{
		api.SubscriptionBinance,
		api.SubscriptionCoingecko,
		api.SubscriptionFixer,
	} {
		wg.Add(1)
		go func(subscriber daemon.AggFunc) {
			defer wg.Done()
			SubscriptionWorker(ctx, storeHandler.Logger, storeHandler.Cfg, subscriber)
		}(s)
	}
	wg.Wait()
}

func SubscriptionWorker(ctx context.Context, logger *zap.SugaredLogger, cfg *config.Config, fn daemon.AggFunc) {
	interval, err := time.ParseDuration(cfg.Interval)
	if err != nil {
		logger.Errorw("PriceProvider", "SubscriptionWorker", err)
		return
	}

	logger.Infow("PriceProvider", "SubscriptionWorker", "Start", "subscription function", daemon.GetFunctionName(fn))
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := fn(); err != nil {
			logger.Errorw("PriceProvider", "SubscriptionWorker function name:", daemon.GetFunctionName(fn), "Error:", err)
		}
		time.Sleep(interval)
	}
}

func (api *Api) SubscriptionBinance() error {
	whitelistedTokens, err := api.StoreHandler.GetCNSWhitelistedTokens()
	if err != nil {
		return fmt.Errorf("SubscriptionBinance, GetCNSWhitelistedTokens(): %w", err)
	}
	if len(whitelistedTokens) == 0 {
		return fmt.Errorf("SubscriptionBinance: No whitelisted tokens")
	}
	for _, token := range whitelistedTokens {
		tokenSymbol := token + types.USDT
		req, err := http.NewRequest("GET", BinanceURL, nil)
		if err != nil {
			return fmt.Errorf("SubscriptionBinance: fetch binance: %w", err)
		}

		q := url.Values{}
		q.Add("symbol", tokenSymbol)
		req.Header.Set("Accepts", "application/json")
		req.URL.RawQuery = q.Encode()

		resp, err := api.Client.Do(req)
		if err != nil {
			return fmt.Errorf("SubscriptionBinance: fetch binance: %w", err)
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("SubscriptionBinance: read body: %w", err)
		}
		if err := resp.Body.Close(); err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			if resp.StatusCode == http.StatusBadRequest {
				api.StoreHandler.Logger.Infof("SubscriptionBinance: %s, Status: %s, Symbol: %s", body, resp.Status, tokenSymbol)
				continue
			}
			return fmt.Errorf("SubscriptionBinance: %s, Status: %s, Symbol: %s", body, resp.Status, tokenSymbol)
		}

		if err := resp.Body.Close(); err != nil {
			return err
		}

		bp := types.Binance{}
		if err = json.Unmarshal(body, &bp); err != nil {
			return fmt.Errorf("SubscriptionBinance: unmarshal body: %w", err)
		}

		strToFloat, err := strconv.ParseFloat(bp.Price, 64)
		if err != nil {
			return fmt.Errorf("SubscriptionBinance: convert price to float: %w", err)
		}
		if strToFloat == float64(0) {
			continue
		}

		now := time.Now()
		if err = api.StoreHandler.Store.UpsertToken(store.BinanceStore, bp.Symbol, strToFloat, now.Unix()); err != nil {
			return fmt.Errorf("SubscriptionBinance, Store.UpsertToken(%s,%s,%f): %w", store.BinanceStore, bp.Symbol, strToFloat, err)
		}
		time.Sleep(1 * time.Second)
	}
	return nil
}

func (api *Api) SubscriptionCoingecko() error {
	whitelistedTokens, err := api.StoreHandler.CNSPriceIdQuery()
	if err != nil {
		return fmt.Errorf("SubscriptionCoingecko, CNSPriceIdQuery(): %w", err)
	}
	if len(whitelistedTokens) == 0 {
		return fmt.Errorf("SubscriptionCoingecko: No whitelisted tokens")
	}

	cg := gecko.NewClient(api.Client)
	pcp := geckoTypes.PriceChangePercentageObject
	priceChangePercentage := []string{pcp.PCP1h}
	order := geckoTypes.OrderTypeObject.MarketCapDesc
	market, err := cg.CoinsMarket(types.USD, whitelistedTokens, order, 1, 1, false, priceChangePercentage)
	if err != nil {
		return fmt.Errorf("SubscriptionCoingecko, cg.CoinsMarket(): %w", err)
	}

	for _, token := range *market {
		tokenSymbol := strings.ToUpper(token.Symbol) + types.USDT

		now := time.Now()

		if err = api.StoreHandler.Store.UpsertToken(store.CoingeckoStore, tokenSymbol, token.CurrentPrice, now.Unix()); err != nil {
			return fmt.Errorf("SubscriptionCoingecko, Store.UpsertToken(%s,%s,%f): %w", store.CoingeckoStore, tokenSymbol, token.CurrentPrice, err)
		}

		if err = api.StoreHandler.Store.UpsertTokenSupply(store.CoingeckoSupplyStore, tokenSymbol, token.CirculatingSupply); err != nil {
			return fmt.Errorf("SubscriptionCoingecko, Store.UpsertTokenSupply(%s,%s,%f): %w", store.CoingeckoSupplyStore, tokenSymbol, token.CirculatingSupply, err)
		}
		time.Sleep(1 * time.Second)
	}
	return nil
}

func (api *Api) SubscriptionFixer() error {
	req, err := http.NewRequest("GET", FixerURL, nil)
	if err != nil {
		return fmt.Errorf("SubscriptionFixer: fetch Fixer: %w", err)
	}
	q := url.Values{}
	q.Add("access_key", api.StoreHandler.Cfg.FixerApiKey)
	q.Add("base", types.USD)
	q.Add("symbols", strings.Join(api.StoreHandler.Cfg.WhitelistedFiats, ","))

	req.URL.RawQuery = q.Encode()

	resp, err := api.Client.Do(req)
	if err != nil {
		return fmt.Errorf("SubscriptionFixer: fetch Fixer: %w", err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("SubscriptionFixer: read body: %w", err)
	}
	if err := resp.Body.Close(); err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("SubscriptionFixer: %s, Status: %s", body, resp.Status)
	}

	bp := types.Fixer{}
	if err = json.Unmarshal(body, &bp); err != nil {
		return fmt.Errorf("SubscriptionFixer: unmarshal body: %w", err)
	}
	if !bp.Success {
		api.StoreHandler.Logger.Infow("SubscriptionFixer", "The status message of the query is fail(Maybe the apikey problem)", bp.Success)
		return nil
	}
	var data map[string]float64
	if err = json.Unmarshal(bp.Rates, &data); err != nil {
		return fmt.Errorf("SubscriptionFixer: unmarshal body: %w", err)
	}

	for _, fiat := range api.StoreHandler.Cfg.WhitelistedFiats {
		fiatSymbol := types.USD + fiat
		d, ok := data[fiat]
		if !ok {
			api.StoreHandler.Logger.Infow("SubscriptionFixer", "From the provider list of deliveries price for symbol not found", fiatSymbol)
			return nil
		}

		now := time.Now()
		if err = api.StoreHandler.Store.UpsertToken(store.FixerStore, fiatSymbol, d, now.Unix()); err != nil {
			return fmt.Errorf("SubscriptionFixer, Store.UpsertToken(%s,%s,%f): %w", store.FixerStore, fiatSymbol, d, err)
		}
	}
	return nil
}
