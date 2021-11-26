package priceprovider

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/allinbits/emeris-price-oracle/price-oracle/daemon"
	"github.com/allinbits/emeris-price-oracle/price-oracle/store"
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
	BinanceURL = "https://api.binance.com/api/v3/ticker/price"
	FixerURL   = "https://data.fixer.io/api/latest"

	//CoinmarketcapURL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
)

type Api struct {
	Client       *http.Client
	StoreHandler *store.Handler
}

func StartSubscription(ctx context.Context, storeHandler *store.Handler, logger *zap.SugaredLogger, cfg *config.Config) {
	api := Api{
		Client:       &http.Client{Timeout: 2 * time.Second},
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
			SubscriptionWorker(ctx, logger, cfg, subscriber)
		}(s)
	}

	wg.Wait()
}

func SubscriptionWorker(ctx context.Context, logger *zap.SugaredLogger, cfg *config.Config, fn daemon.AggFunc) {
	logger.Infow("INFO", "Database", "SubscriptionWorker Start")
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := fn(); err != nil {
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

func (api *Api) SubscriptionBinance() error {
	whitelistTokens, err := api.StoreHandler.GetCNSWhitelistedTokens()
	if err != nil {
		return fmt.Errorf("SubscriptionBinance GetCNSWhitelistedTokens: %w", err)
	}
	if len(whitelistTokens) == 0 {
		return fmt.Errorf("SubscriptionBinance GetCNSWhitelistedTokens: The token does not exist")
	}
	for _, token := range whitelistTokens {
		tokenSymbol := token + types.USDT
		req, err := http.NewRequest("GET", BinanceURL, nil)
		if err != nil {
			return fmt.Errorf("SubscriptionBinance fetch binance: %w", err)
		}

		q := url.Values{}
		q.Add("symbol", tokenSymbol)
		req.Header.Set("Accepts", "application/json")
		req.URL.RawQuery = q.Encode()

		resp, err := api.Client.Do(req)
		if err != nil {
			return fmt.Errorf("SubscriptionBinance fetch binance: %w", err)
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("SubscriptionBinance read body: %w", err)
		}
		if err := resp.Body.Close(); err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			if resp.StatusCode == http.StatusBadRequest {
				api.StoreHandler.Logger.Infof("SubscriptionBinance: %s, Status: %s", body, resp.Status)
				continue
			}
			return fmt.Errorf("SubscriptionBinance: %s, Status: %s", body, resp.Status)
		}

		if err := resp.Body.Close(); err != nil {
			return err
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
		if err = api.StoreHandler.Store.UpsertToken(store.BinanceStore, bp.Symbol, strToFloat, now.Unix()); err != nil {
			return fmt.Errorf("SubscriptionBinance, Store.UpsertToken(%s,%s,%f): %w", store.BinanceStore, bp.Symbol, strToFloat, err)
		}
		time.Sleep(1 * time.Second)
	}
	return nil
}

func (api *Api) SubscriptionCoingecko() error {
	whitelistTokens, err := api.StoreHandler.CnsPriceIdQuery()
	if err != nil {
		return fmt.Errorf("SubscriptionCoingecko CnsPriceIdQuery: %w", err)
	}
	if len(whitelistTokens) == 0 {
		return fmt.Errorf("SubscriptionCoingecko CnsPriceIdQuery: The token does not exist")
	}

	cg := gecko.NewClient(api.Client)
	vsCurrency := types.USD
	perPage := 1
	page := 1
	pcp := geckoTypes.PriceChangePercentageObject
	priceChangePercentage := []string{pcp.PCP1h}
	order := geckoTypes.OrderTypeObject.MarketCapDesc
	market, err := cg.CoinsMarket(vsCurrency, whitelistTokens, order, perPage, page, false, priceChangePercentage)
	if err != nil {
		return fmt.Errorf("SubscriptionCoingecko Market Query: %w", err)
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
		return fmt.Errorf("SubscriptionFixer fetch Fixer: %w", err)
	}
	q := url.Values{}
	q.Add("access_key", api.StoreHandler.Cfg.Fixerapikey)
	q.Add("base", types.USD)
	q.Add("symbols", strings.Join(api.StoreHandler.Cfg.Whitelistfiats, ","))

	req.URL.RawQuery = q.Encode()

	resp, err := api.Client.Do(req)
	if err != nil {
		return fmt.Errorf("SubscriptionFixer fetch Fixer: %w", err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("SubscriptionFixer read body: %w", err)
	}
	if err := resp.Body.Close(); err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("SubscriptionFixer: %s, Status: %s", body, resp.Status)
	}

	if err := resp.Body.Close(); err != nil {
		return err
	}

	bp := types.Fixer{}
	if err = json.Unmarshal(body, &bp); err != nil {
		return fmt.Errorf("SubscriptionFixer unmarshal body: %w", err)
	}
	if !bp.Success {
		api.StoreHandler.Logger.Infow("SubscriptionFixer", "The status message of the query is fail(Maybe the apikey problem)", bp.Success)
		return nil
	}
	var data map[string]float64
	if err = json.Unmarshal(bp.Rates, &data); err != nil {
		return fmt.Errorf("SubscriptionFixer unmarshal body: %w", err)
	}

	for _, fiat := range api.StoreHandler.Cfg.Whitelistfiats {
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
