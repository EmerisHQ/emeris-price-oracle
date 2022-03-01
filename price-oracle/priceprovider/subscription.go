package priceprovider

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
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
		Client: &http.Client{
			Timeout: storeHandler.Cfg.HttpClientTimeout,
			Transport: &http.Transport{
				IdleConnTimeout:       storeHandler.Cfg.HttpClientTimeout * 2,
				ResponseHeaderTimeout: storeHandler.Cfg.HttpClientTimeout,
				DialContext: (&net.Dialer{
					Timeout:   storeHandler.Cfg.HttpClientTimeout,
					KeepAlive: storeHandler.Cfg.HttpClientTimeout * 2,
				}).DialContext,
			},
		},
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

	req, err := http.NewRequest("GET", BinanceURL, nil)
	if err != nil {
		return fmt.Errorf("SubscriptionBinance: fetch binance: %w", err)
	}
	req.Header.Set("Accepts", "application/json")

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
		return fmt.Errorf("SubscriptionBinance %s Status: %s", body, resp.Status)
	}
	var resList []types.Binance
	if err = json.Unmarshal(body, &resList); err != nil {
		return fmt.Errorf("SubscriptionBinance: unmarshal body: %w", err)
	}

	// Binance response: [{"symbol":"ETHUSDT","price":"4021.067276"},{"symbol":"LTCUSDT","price":"200.00250"},...]
	// Convert it to: { "ETHUSDT": 4021.067276, "LTCUSDT": 200.00250 }
	symbolPriceMap := make(map[string]float64)
	for _, b := range resList {
		val, err := strconv.ParseFloat(b.Price, 64)
		if err != nil {
			api.StoreHandler.Logger.Errorw("SubscriptionBinance", "ParseFloat", err)
			continue
		}
		symbolPriceMap[strings.ToUpper(b.Symbol)] = val // Force Upper, better safe than sorry!
	}

	var missingTokens []string
	for _, token := range whitelistedTokens {
		tokenSymbol := strings.ToUpper(token + types.USDT) // Force upper, better safe than sorry!
		var price float64
		var ok bool
		if price, ok = symbolPriceMap[tokenSymbol]; !ok {
			missingTokens = append(missingTokens, tokenSymbol)
			continue
		}
		now := time.Now()
		if err = api.StoreHandler.Store.UpsertToken(store.BinanceStore, tokenSymbol, price, now.Unix()); err != nil {
			return fmt.Errorf("SubscriptionBinance, Store.UpsertToken(%s,%s,%f): %w", store.BinanceStore, tokenSymbol, price, err)
		}
		time.Sleep(1 * time.Second)
	}
	if len(missingTokens) > 0 {
		api.StoreHandler.Logger.Infow("SubscriptionBinance", "MissingTokens", strings.Join(missingTokens, ", "))
	}
	return nil
}

func (api *Api) SubscriptionCoingecko() error {
	pidToTickers, err := api.StoreHandler.GetCNSPriceIdsToTicker()
	if err != nil {
		return fmt.Errorf("SubscriptionCoingecko, GetCNSPriceIdsToTicker(): %w", err)
	}
	if len(pidToTickers) == 0 {
		return fmt.Errorf("SubscriptionCoingecko: No whitelisted tokens")
	}

	priceIds := make([]string, len(pidToTickers))
	for p := range pidToTickers {
		priceIds = append(priceIds, p)
	}
	api.StoreHandler.Logger.Infow("SubscriptionCoingecko", "Calling Price Ids", strings.Join(priceIds, ", "))

	cg := gecko.NewClient(api.Client)
	pcp := geckoTypes.PriceChangePercentageObject
	priceChangePercentage := []string{pcp.PCP1h}
	order := geckoTypes.OrderTypeObject.MarketCapDesc
	market, err := cg.CoinsMarket(types.USD, priceIds, order, 1, 1, false, priceChangePercentage)
	if err != nil {
		return fmt.Errorf("SubscriptionCoingecko, cg.CoinsMarket(): %w", err)
	}

	respTokenSymbols := make([]string, 0, len(*market)) // For logging.
	for _, token := range *market {
		tokenSymbol := strings.ToUpper(token.Symbol) + types.USDT
		respTokenSymbols = append(respTokenSymbols, fmt.Sprintf("(ID: %s Symbol: %s)", token.ID, token.Symbol))
		now := time.Now().Round(0)

		if err = api.StoreHandler.Store.UpsertToken(store.CoingeckoStore, tokenSymbol, token.CurrentPrice, now.Unix()); err != nil {
			return fmt.Errorf("SubscriptionCoingecko, Store.UpsertToken(%s,%s,%f): %w", store.CoingeckoStore, tokenSymbol, token.CurrentPrice, err)
		}

		if err = api.StoreHandler.Store.UpsertTokenSupply(store.CoingeckoSupplyStore, tokenSymbol, token.CirculatingSupply); err != nil {
			return fmt.Errorf("SubscriptionCoingecko, Store.UpsertTokenSupply(%s,%s,%f): %w", store.CoingeckoSupplyStore, tokenSymbol, token.CirculatingSupply, err)
		}
		time.Sleep(1 * time.Second)
	}
	api.StoreHandler.Logger.Infow("SubscriptionCoingecko", "Received Price Ids", strings.Join(respTokenSymbols, ", "))
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
