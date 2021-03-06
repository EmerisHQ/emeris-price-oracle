package store

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"

	"github.com/getsentry/sentry-go"
	gecko "github.com/superoo7/go-gecko/v3"
	geckoTypes "github.com/superoo7/go-gecko/v3/types"

	"github.com/emerishq/emeris-price-oracle/price-oracle/config"
	"github.com/emerishq/emeris-price-oracle/price-oracle/types"
	"github.com/emerishq/emeris-utils/sentryx"
	"go.uber.org/zap"

	"time"
)

type Store interface {
	Init(context.Context) error // runs migrations
	Close() error
	GetTokenPriceAndSupplies(ctx context.Context, tokens []string) ([]types.TokenPriceAndSupply, error)
	GetFiatPrices(ctx context.Context, fiats []string) ([]types.FiatPrice, error)
	GetTokenNames(ctx context.Context) ([]string, error)
	GetPriceIDToTicker(context.Context) (map[string]string, error)
	GetPrices(ctx context.Context, from string) ([]types.Prices, error)
	UpsertPrice(ctx context.Context, to string, price float64, token string) error
	UpsertToken(ctx context.Context, to string, symbol string, price float64, time int64) error
	UpsertTokenSupply(ctx context.Context, to string, symbol string, supply float64) error
}

const (
	BinanceStore         = "oracle.binance"
	CoingeckoStore       = "oracle.coingecko"
	FixerStore           = "oracle.fixer"
	TokensStore          = "oracle.tokens"
	FiatsStore           = "oracle.fiats"
	CoingeckoSupplyStore = "oracle.coingeckosupply"

	GranularityMinute = "5M"
	GranularityHour   = "1H"
	GranularityDay    = "1D"
)

type Handler struct {
	Store      Store
	Logger     *zap.SugaredLogger
	Cfg        *config.Config
	SpotCache  *TokenAndFiatCache
	ChartCache *ChartDataCache

	// token gecko symbol aka ticker aka name -> gecko id
	GeckoIdCache *sync.Map
}

type TokenAndFiatCache struct {
	// cosmos -> atom; osmosis -> osmo ...
	PriceIDtoTicker       map[string]string
	WhitelistedTickers    []string
	TokenPriceAndSupplies map[string]types.TokenPriceAndSupply
	FiatPrices            map[string]types.FiatPrice

	RefreshInterval time.Duration
	Mu              sync.RWMutex
}

// ChartDataCache is holder of chart data in a map and evacuating the cache
// in every 5M, 1H and 1D depending on what data it's holding. Data is a map
// that holds another map that holds a geckoTypes.CoinsIDMarketChart type.
//
// Couple of example of ChartDataCache can be:
// [5M][cosmos-usd] -> geckoTypes.CoinsIDMarketChart{...}
// [1D][bitcoin-eur] -> geckoTypes.CoinsIDMarketChart{...}
// [1D][cosmos-usd] -> geckoTypes.CoinsIDMarketChart{...}
// Where 1D means it's a one-day granularity data. bitcoin/cosmos is the key
// for the second map, which holds geckoTypes.CoinsIDMarketChart as value.
// geckoTypes.CoinsIDMarketChart holds 3 lists of geckoTypes.ChartItems
// which is a native coinGecko type that is basically a [2]float32, where
// the zero index represent the unix timestamp and the first index is the value.
//
// Discussion can be found in this GH issue:
// https://github.com/emerishq/demeris-backend/issues/109#issuecomment-993513347
//
// RefreshInterval is always 5 minutes. To know why, follow the description of
// GetChartData function.
type ChartDataCache struct {
	Data            map[string]map[string]*geckoTypes.CoinsIDMarketChart
	Mu              sync.RWMutex
	RefreshInterval time.Duration
}

type option func(*Handler) error

func WithDB(ctx context.Context, store Store) func(*Handler) error {
	return func(handler *Handler) error {
		if store == nil {
			return fmt.Errorf("received nil reference for SqlDB")
		}
		handler.Store = store
		return handler.Store.Init(ctx) // Init the DB i.e. Run migrations.
	}
}

func WithLogger(logger *zap.SugaredLogger) func(*Handler) error {
	return func(handler *Handler) error {
		if logger == nil {
			return fmt.Errorf("received nil reference for logger")
		}
		handler.Logger = logger
		return nil
	}
}

func WithConfig(cfg *config.Config) func(*Handler) error {
	return func(handler *Handler) error {
		if cfg == nil {
			return fmt.Errorf("received nil reference for config")
		}
		handler.Cfg = cfg
		return nil
	}
}

func WithSpotPriceCache(cache *TokenAndFiatCache) func(*Handler) error {
	return func(handler *Handler) error {
		if cache == nil {
			cache = &TokenAndFiatCache{
				PriceIDtoTicker:       nil,
				WhitelistedTickers:    nil,
				TokenPriceAndSupplies: nil,
				FiatPrices:            nil,
				RefreshInterval:       time.Second * 5,
				Mu:                    sync.RWMutex{},
			}
		}
		handler.SpotCache = cache
		// Invalidate in-memory cache after RefreshInterval
		go func(cache *TokenAndFiatCache) {
			randomInt := rand.New(rand.NewSource(time.Now().UnixNano())).Int63n(5) + 5 //nolint:gosec
			d := cache.RefreshInterval + (cache.RefreshInterval / time.Duration(randomInt))
			//nolint
			for {
				select {
				case <-time.Tick(d):
					cache.Mu.Lock()
					cache.PriceIDtoTicker = nil
					cache.WhitelistedTickers = nil
					cache.FiatPrices = nil
					cache.TokenPriceAndSupplies = nil
					cache.Mu.Unlock()
				}
			}
		}(handler.SpotCache)
		return nil
	}
}

func WithChartDataCache(cache *ChartDataCache, refresh time.Duration) func(*Handler) error {
	return func(handler *Handler) error {
		if cache == nil {
			cache = &ChartDataCache{
				Data:            map[string]map[string]*geckoTypes.CoinsIDMarketChart{},
				Mu:              sync.RWMutex{},
				RefreshInterval: refresh,
			}
		}
		handler.ChartCache = cache

		// Invalidate in-memory cache for chart data after RefreshInterval
		go func(cache *ChartDataCache) {
			//nolint
			for {
				select {
				case tm := <-time.Tick(cache.RefreshInterval):
					cache.Mu.Lock()
					cache.Data[GranularityMinute] = nil
					// Minute return an int value in [0, 59]
					// Ticker is 5 minutes, let's say we start at minute 33
					// Tick cycle will be:
					// 38, 43, 48, 53, 58, 3, 8, 13, 18, 23, 28, 33, 38 ...
					// so, <5 means it's the beginning of the hour.
					if tm.Minute() < 5 {
						cache.Data[GranularityHour] = nil
					}
					// Hour returns an int in [0, 23]
					// so, 0 means beginning of the day.
					if tm.Hour() == 0 && tm.Minute() < 5 {
						cache.Data[GranularityDay] = nil
					}
					cache.Mu.Unlock()
				}
			}
		}(handler.ChartCache)
		return nil
	}
}

// NewStoreHandler takes a list of options and builds the handler. Some
// properties of the handler require validation and(or) error check, those
// properties are coming via param: <options>.
//
// Simple properties are initialised inline. GeckoIdCache for example.
//
// Store        : Interface to query the DB + caches combo. Store is populated
//                by WithDB(store) function which runs necessary DB migrations.
// Logger       : We're using zap now. No plan to change in foreseeable future.
// Cfg          : All configs. Important ones are Http timeout and conn string for DB.
// SpotCache    : This is the cache sits in front the DB. Some functions of
//                Store interface queries this caches first.
// ChartCache   : Historical price data.
// GeckoIdCache : Coin Gecko used coin id to query them. Others use coin ticker.
//                So we cache coin ids.
func NewStoreHandler(options ...option) (*Handler, error) {
	handler := &Handler{
		Store:      nil,
		Logger:     nil,
		Cfg:        nil,
		SpotCache:  nil,
		ChartCache: nil,
		// Don't need error check or validation. So inline init is enough.
		GeckoIdCache: &sync.Map{},
	}
	for _, opt := range options {
		if err := opt(handler); err != nil {
			return nil, fmt.Errorf("option failed: %w", err)
		}
	}
	return handler, nil
}

// GetGeckoIdForTokenNames takes a list of token names/ticker ("symbol" in coin-gecko's definition)
// and returns a map (name AKA ticker -> gecko id). This id is used to query coin gecko api. As
// coin gecko takes id as api query param. (Most other platforms take name aka ticker.)
//
// CNS DB must be updated with correct price_id -> ticker relations. If not fix it on CNS.
// We fetch those relations, inverse those relations and serve with the best effort. If we
// don't have some name present in the CNS DB, log and continue.
//
// If the (param<name>) is empty, return all.
func (h *Handler) GetGeckoIdForTokenNames(ctx context.Context, names []string) (map[string]string, error) {
	pidToTicker, err := h.GetCNSPriceIdsToTicker(ctx)
	if err != nil {
		return nil, err
	}
	// pidToTicker is price_id -> ticker AKA name AKA symbol
	// Example: cosmos -> atom, osmosis -> osmo, terra-luna -> luna
	//
	// But we want the opposite: luna -> terra-luna; osmo -> osmosis ...
	tickerToPid := make(map[string]string, len(pidToTicker))
	for p, t := range pidToTicker {
		tickerToPid[t] = p
	}

	if len(names) == 0 { // return all
		return tickerToPid, nil
	}

	for i, n := range names {
		names[i] = strings.ToLower(n)
	}

	ret := make(map[string]string)
	for _, n := range names {
		ret[n] = tickerToPid[n]
	}
	return ret, nil
}

// GetCNSWhitelistedTokens returns the whitelisted tokens.
// It first checks the in-memory cache.
// If cache is nil, it fetches and updates the cache.
func (h *Handler) GetCNSWhitelistedTokens(ctx context.Context) ([]string, error) {
	var tokens []string
	h.SpotCache.Mu.RLock()
	if h.SpotCache.WhitelistedTickers != nil {
		tokens = append([]string(nil), h.SpotCache.WhitelistedTickers...)
	}
	h.SpotCache.Mu.RUnlock()
	if len(tokens) != 0 {
		return tokens, nil
	}

	names, err := h.Store.GetTokenNames(ctx)
	if err != nil {
		return nil, err
	}
	h.SpotCache.Mu.Lock()
	h.SpotCache.WhitelistedTickers = append([]string(nil), names...)
	h.SpotCache.Mu.Unlock()

	return names, nil
}

func (h *Handler) GetCNSPriceIdsToTicker(ctx context.Context) (map[string]string, error) {
	h.SpotCache.Mu.RLock()
	pidToTkr := make(map[string]string, len(h.SpotCache.PriceIDtoTicker))
	if h.SpotCache.PriceIDtoTicker != nil {
		for p, t := range h.SpotCache.PriceIDtoTicker {
			pidToTkr[p] = t
		}
	}
	h.SpotCache.Mu.RUnlock()
	if len(pidToTkr) != 0 {
		return pidToTkr, nil
	}

	idsToTicker, err := h.Store.GetPriceIDToTicker(ctx)
	if err != nil {
		return nil, err
	}
	h.SpotCache.Mu.Lock()
	h.SpotCache.PriceIDtoTicker = idsToTicker
	h.SpotCache.Mu.Unlock()

	return idsToTicker, nil
}

// GetTokenPriceAndSupplies returns a list of TokenPriceAndSupply. It first
// checks if in-memory cache is still valid and all requested tokens are cached.
// If not it fetches all the requested tokens and updates the cache.
func (h *Handler) GetTokenPriceAndSupplies(ctx context.Context, tokens []string) ([]types.TokenPriceAndSupply, error) {
	h.SpotCache.Mu.RLock()
	cachedTokens := make([]string, 0, len(h.SpotCache.TokenPriceAndSupplies))
	for t := range h.SpotCache.TokenPriceAndSupplies {
		cachedTokens = append(cachedTokens, t)
	}
	h.SpotCache.Mu.RUnlock()

	if h.SpotCache.TokenPriceAndSupplies == nil || !isSubset(tokens, cachedTokens) {
		tokensDetails, err := h.Store.GetTokenPriceAndSupplies(ctx, tokens)
		if err != nil {
			return nil, err
		}

		h.SpotCache.Mu.Lock()
		if h.SpotCache.TokenPriceAndSupplies == nil {
			h.SpotCache.TokenPriceAndSupplies = make(map[string]types.TokenPriceAndSupply, len(tokensDetails))
		}
		for _, t := range tokensDetails {
			h.SpotCache.TokenPriceAndSupplies[t.Symbol] = t
		}
		h.SpotCache.Mu.Unlock()
		return tokensDetails, err
	}

	h.SpotCache.Mu.RLock()
	tokenDetails := make([]types.TokenPriceAndSupply, 0, len(tokens))
	for _, t := range tokens {
		tokenDetails = append(tokenDetails, h.SpotCache.TokenPriceAndSupplies[t])
	}
	h.SpotCache.Mu.RUnlock()
	return tokenDetails, nil
}

// GetFiatPrices returns a list of FiatPrice. It first checks if
// in-memory cache is still valid and all requested tokens are cached.
// If not it fetches all the requested tokens and updates the cache.
func (h *Handler) GetFiatPrices(ctx context.Context, fiats []string) ([]types.FiatPrice, error) {
	h.SpotCache.Mu.RLock()
	cachedFiats := make([]string, 0, len(h.SpotCache.FiatPrices))
	for f := range h.SpotCache.FiatPrices {
		cachedFiats = append(cachedFiats, f)
	}
	h.SpotCache.Mu.RUnlock()

	if len(cachedFiats) == 0 || !isSubset(fiats, cachedFiats) {
		fiatPrices, err := h.Store.GetFiatPrices(ctx, fiats)
		if err != nil {
			return nil, err
		}

		h.SpotCache.Mu.Lock()
		if h.SpotCache.FiatPrices == nil {
			h.SpotCache.FiatPrices = make(map[string]types.FiatPrice, len(fiatPrices))
		}
		for _, f := range fiatPrices {
			h.SpotCache.FiatPrices[f.Symbol] = f
		}
		h.SpotCache.Mu.Unlock()
		return fiatPrices, nil
	}
	h.SpotCache.Mu.RLock()
	fiatPrices := make([]types.FiatPrice, 0, len(fiats))
	for _, f := range fiats {
		fiatPrices = append(fiatPrices, h.SpotCache.FiatPrices[f])
	}
	h.SpotCache.Mu.RUnlock()
	return fiatPrices, nil
}

func (h *Handler) GetChartData(
	ctx context.Context,
	coinId string,
	days string,
	currency string,
	geckoClient *gecko.Client,
) (*geckoTypes.CoinsIDMarketChart, error) {
	var granularity, maxFetchDays string
	switch days {
	case "1":
		granularity = GranularityMinute
		maxFetchDays = "1"
	case "7", "14", "30", "90":
		granularity = GranularityHour
		maxFetchDays = "90"
	default:
		granularity = GranularityDay
		maxFetchDays = "max"
	}

	var err error
	coinIDCurrency := fmt.Sprintf("%s-%s", coinId, currency)
	h.ChartCache.Mu.Lock()
	chartData, ok := h.ChartCache.Data[granularity][coinIDCurrency]
	if !ok {
		chartData, err = geckoClient.CoinsIDMarketChart(coinId, currency, maxFetchDays)
		if err != nil {
			h.ChartCache.Mu.Unlock() // unlock mutex
			return nil, err
		}
		if h.ChartCache.Data[granularity] == nil {
			h.ChartCache.Data[granularity] = map[string]*geckoTypes.CoinsIDMarketChart{}
		}
		h.ChartCache.Data[granularity][coinIDCurrency] = chartData
	}
	h.ChartCache.Mu.Unlock() // unlock mutex

	if days == "1" || days == "max" {
		return chartData, nil
	}
	// Since we've covered the "max" case, daysInt now can only have values: 7, 14, 30, 90, 180, 365
	daysInt, err := strconv.Atoi(days)
	if err != nil {
		return nil, err
	}
	sliceLimit := daysInt
	if daysInt <= 90 {
		// When 1 < days <= 90; data granularity is by hour.
		sliceLimit = daysInt * 24
	}

	// Serve with the best effort! If we don't have all the data
	// response with what we have.
	//
	// This should not occur in real life. Only for test.
	n := len(*(chartData.Prices))
	if sliceLimit > n {
		sliceLimit = n
	}
	// Coin gecko return data as ascending order of timestamp
	// i.e. the latest data are at the end of the list.
	prices := (*chartData.Prices)[n-sliceLimit:]
	marketCap := (*chartData.MarketCaps)[n-sliceLimit:]
	volume := (*chartData.TotalVolumes)[n-sliceLimit:]

	return &geckoTypes.CoinsIDMarketChart{
		Prices:       &prices,
		MarketCaps:   &marketCap,
		TotalVolumes: &volume,
	}, nil
}

func (h *Handler) PriceTokenAggregator(ctx context.Context) error {
	span, ctx := sentryx.StartSpan(ctx, "aggregator", sentry.TransactionName("PriceTokenAggregator"))
	defer span.Finish()

	// symbolKV[Symbol][Store]=price
	symbolKV := make(map[string]map[string]float64)
	stores := []string{BinanceStore, CoingeckoStore}

	whitelist := make(map[string]struct{})
	cnsWhitelist, err := h.GetCNSWhitelistedTokens(ctx)
	if err != nil {
		return fmt.Errorf("GetCNSWhitelistedTokens: %w", err)
	}
	for _, token := range cnsWhitelist {
		baseToken := token + types.USDT
		whitelist[baseToken] = struct{}{}
	}

	for _, s := range stores {
		prices, err := h.Store.GetPrices(ctx, s)
		if err != nil {
			return fmt.Errorf("Store.GetPrices(%s): %w", s, err)
		}
		for _, token := range prices {
			if _, ok := whitelist[token.Symbol]; !ok {
				continue
			}
			now := time.Now()

			// do not update if it was already updated in the last minute
			if token.UpdatedAt < now.Unix()-60 {
				continue
			}
			pricelist, ok := symbolKV[token.Symbol]
			if !ok {
				pricelist = make(map[string]float64)
			}
			pricelist[s] = token.Price
			symbolKV[token.Symbol] = pricelist
		}
	}

	for token := range whitelist {
		if len(symbolKV[token]) == 0 {
			h.Logger.Infow("PriceTokenAggregator", "Price not found for", token)
			continue
		}
		mean, err := Averaging(symbolKV[token])
		if err != nil {
			h.Logger.Errorw("PriceTokenAggregator", "Err:", err, "Token:", token)
			continue // Best effort, update as much as we can.
		}

		if err = h.Store.UpsertPrice(ctx, TokensStore, mean, token); err != nil {
			h.Logger.Errorw("PriceTokenAggregator", "UpsertPrice Err:", err, "Token:", token)
			continue // Best effort, update as much as we can.
		}
	}
	return nil
}

func (h *Handler) PriceFiatAggregator(ctx context.Context) error {
	span, ctx := sentryx.StartSpan(ctx, "aggregator", sentry.TransactionName("PriceFiatAggregator"))
	defer span.Finish()

	// symbolKV[Symbol][Store]=price
	symbolKV := make(map[string]map[string]float64)
	stores := []string{FixerStore}

	whitelist := make(map[string]struct{})
	for _, fiat := range h.Cfg.WhitelistedFiats {
		baseFiat := types.USD + fiat
		whitelist[baseFiat] = struct{}{}
	}

	for _, s := range stores {
		prices, err := h.Store.GetPrices(ctx, s)
		if err != nil {
			return fmt.Errorf("Store.GetPrices(%s): %w", s, err)
		}
		for _, fiat := range prices {
			if _, ok := whitelist[fiat.Symbol]; !ok {
				continue
			}
			now := time.Now()
			if fiat.UpdatedAt < now.Unix()-60 {
				continue
			}
			pricelist, ok := symbolKV[fiat.Symbol]
			if !ok {
				pricelist = make(map[string]float64)
			}
			pricelist[s] = fiat.Price
			symbolKV[fiat.Symbol] = pricelist
		}
	}
	for fiat := range symbolKV {
		if len(symbolKV[fiat]) == 0 {
			h.Logger.Infow("PriceFiatAggregator", "Price not found for", fiat)
			continue
		}
		mean, err := Averaging(symbolKV[fiat])
		if err != nil {
			h.Logger.Errorw("PriceFiatAggregator", "Err:", err, "Fiat:", fiat)
			continue // Best effort, update as much as we can.
		}

		if err := h.Store.UpsertPrice(ctx, FiatsStore, mean, fiat); err != nil {
			h.Logger.Errorw("PriceFiatAggregator", "UpsertPrice Err:", err, "Token:", fiat)
			continue // Best effort, update as much as we can.
		}
	}
	return nil
}

// isSubset returns true if all element of subList in found in globalList
func isSubset(subList []string, globalList []string) bool {
	// Turn globalList into a map
	globalSet := make(map[string]bool, len(globalList))
	for _, s := range globalList {
		globalSet[s] = true
	}

	for _, s := range subList {
		if _, ok := globalSet[s]; !ok {
			return false
		}
	}
	return true
}
