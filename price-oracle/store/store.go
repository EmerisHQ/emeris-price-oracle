package store

import (
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"sync"

	gecko "github.com/superoo7/go-gecko/v3"
	geckoTypes "github.com/superoo7/go-gecko/v3/types"

	"github.com/allinbits/emeris-price-oracle/price-oracle/config"
	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"go.uber.org/zap"

	"time"
)

type Store interface {
	Init() error // runs migrations
	Close() error
	GetTokenPriceAndSupplies(tokens []string) ([]types.TokenPriceAndSupply, error)
	GetFiatPrices(fiats []string) ([]types.FiatPrice, error)
	GetTokenNames() ([]string, error)
	GetPriceIDs() ([]string, error)
	GetPrices(from string) ([]types.Prices, error)
	UpsertPrice(to string, price float64, token string) error
	UpsertToken(to string, symbol string, price float64, time int64) error
	UpsertTokenSupply(to string, symbol string, supply float64) error
	GetGeckoId(names []string, client *http.Client) (map[string]string, error)
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
	Store        Store
	Logger       *zap.SugaredLogger
	Cfg          *config.Config
	SpotCache    *TokenAndFiatCache
	ChartCache   *ChartDataCache
	GeckoIdCache sync.Map
}

type TokenAndFiatCache struct {
	Whitelist             []string
	TokenPriceAndSupplies map[string]types.TokenPriceAndSupply
	FiatPrices            map[string]types.FiatPrice

	RefreshInterval time.Duration
	Mu              sync.Mutex
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
// https://github.com/allinbits/demeris-backend/issues/109#issuecomment-993513347
//
// RefreshInterval is always 5 minutes. To know why, follow the description of
// GetChartData function.
type ChartDataCache struct {
	Data            map[string]map[string]*geckoTypes.CoinsIDMarketChart
	Mu              sync.Mutex
	RefreshInterval time.Duration
}

type option func(*Handler) error

func WithDB(store Store) func(*Handler) error {
	return func(handler *Handler) error {
		if store == nil {
			return fmt.Errorf("received nil reference for SqlDB")
		}
		handler.Store = store
		return nil
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
				Whitelist:             nil,
				TokenPriceAndSupplies: nil,
				FiatPrices:            nil,
				RefreshInterval:       time.Second * 5,
				Mu:                    sync.Mutex{},
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
					cache.Whitelist = nil
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
				Mu:              sync.Mutex{},
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
					// so, 0 means it's the beginning of the hour.
					if tm.Minute() == 0 {
						cache.Data[GranularityHour] = nil
					}
					// Hour returns an int in [0, 23]
					// so, 0 means beginning of the day
					if tm.Hour() == 0 {
						cache.Data[GranularityDay] = nil
					}
					cache.Mu.Unlock()
				}
			}
		}(cache)
		return nil
	}
}

func NewStoreHandler(options ...option) (*Handler, error) {
	handler := &Handler{
		Store:      nil,
		Logger:     nil,
		Cfg:        nil,
		SpotCache:  nil,
		ChartCache: nil,
	}
	for _, opt := range options {
		if err := opt(handler); err != nil {
			return nil, fmt.Errorf("option failed: %w", err)
		}
	}
	if err := handler.Store.Init(); err != nil {
		return nil, err
	}
	return handler, nil
}

func (h *Handler) GetGeckoIdForToken(names []string) ([]string, error) {
	var notCached []string
	for _, name := range names {
		if _, ok := h.GeckoIdCache.Load(name); !ok {
			notCached = append(notCached, name)
		}
	}
	geckoIds, err := h.Store.GetGeckoId(notCached, &http.Client{Timeout: h.Cfg.HttpClientTimeout})
	if err != nil {
		return nil, err
	}
	for name, geckoId := range geckoIds {
		h.GeckoIdCache.Store(name, geckoId)
	}

	var ret []string
	for _, name := range names {
		id, ok := h.GeckoIdCache.Load(name)
		if !ok {
			h.Logger.Errorw("GetGeckoIdForToken", "GeckoId not found for token:", name)
			continue
		}
		ret = append(ret, fmt.Sprintf("%s", id))
	}
	return ret, nil
}

// GetCNSWhitelistedTokens returns the whitelisted tokens.
// It first checks the in-memory cache.
// If cache is nil, it fetches and updates the cache.
func (h *Handler) GetCNSWhitelistedTokens() ([]string, error) {
	if h.SpotCache.Whitelist == nil {
		whitelists, err := h.Store.GetTokenNames()
		if err != nil {
			return nil, err
		}
		h.SpotCache.Mu.Lock()
		h.SpotCache.Whitelist = whitelists
		h.SpotCache.Mu.Unlock()
	}
	return h.SpotCache.Whitelist, nil
}

func (h *Handler) CNSPriceIdQuery() ([]string, error) {
	whitelists, err := h.Store.GetPriceIDs()
	if err != nil {
		return nil, err
	}
	return whitelists, nil
}

// GetTokenPriceAndSupplies returns a list of TokenPriceAndSupply. It first
// checks if in-memory cache is still valid and all requested tokens are cached.
// If not it fetches all the requested tokens and updates the cache.
func (h *Handler) GetTokenPriceAndSupplies(tokens []string) ([]types.TokenPriceAndSupply, error) {
	cachedTokens := make([]string, 0, len(h.SpotCache.TokenPriceAndSupplies))
	for t := range h.SpotCache.TokenPriceAndSupplies {
		cachedTokens = append(cachedTokens, t)
	}

	if h.SpotCache.TokenPriceAndSupplies == nil || !isSubset(tokens, cachedTokens) {
		tokensDetails, err := h.Store.GetTokenPriceAndSupplies(tokens)
		if err != nil {
			return nil, err
		}

		if h.SpotCache.TokenPriceAndSupplies == nil {
			h.SpotCache.TokenPriceAndSupplies = make(map[string]types.TokenPriceAndSupply, len(tokensDetails))
		}
		h.SpotCache.Mu.Lock()
		for _, t := range tokensDetails {
			h.SpotCache.TokenPriceAndSupplies[t.Symbol] = t
		}
		h.SpotCache.Mu.Unlock()
		return tokensDetails, err
	}

	tokenDetails := make([]types.TokenPriceAndSupply, 0, len(tokens))
	for _, t := range tokens {
		tokenDetails = append(tokenDetails, h.SpotCache.TokenPriceAndSupplies[t])
	}
	return tokenDetails, nil
}

// GetFiatPrices returns a list of FiatPrice. It first checks if
// in-memory cache is still valid and all requested tokens are cached.
// If not it fetches all the requested tokens and updates the cache.
func (h *Handler) GetFiatPrices(fiats []string) ([]types.FiatPrice, error) {
	cachedFiats := make([]string, 0, len(h.SpotCache.FiatPrices))
	for f := range h.SpotCache.FiatPrices {
		cachedFiats = append(cachedFiats, f)
	}

	if h.SpotCache.FiatPrices == nil || !isSubset(fiats, cachedFiats) {
		fiatPrices, err := h.Store.GetFiatPrices(fiats)
		if err != nil {
			return nil, err
		}

		if h.SpotCache.FiatPrices == nil {
			h.SpotCache.FiatPrices = make(map[string]types.FiatPrice, len(fiatPrices))
		}
		h.SpotCache.Mu.Lock()
		for _, f := range fiatPrices {
			h.SpotCache.FiatPrices[f.Symbol] = f
		}
		h.SpotCache.Mu.Unlock()
		return fiatPrices, nil
	}
	fiatPrices := make([]types.FiatPrice, 0, len(fiats))
	for _, f := range fiats {
		fiatPrices = append(fiatPrices, h.SpotCache.FiatPrices[f])
	}
	return fiatPrices, nil
}

func (h *Handler) GetChartData(
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
	if sliceLimit > len(*(chartData.Prices)) {
		sliceLimit = len(*(chartData.Prices))
	}
	prices := (*chartData.Prices)[:sliceLimit]
	marketCap := (*chartData.MarketCaps)[:sliceLimit]
	volume := (*chartData.TotalVolumes)[:sliceLimit]

	return &geckoTypes.CoinsIDMarketChart{
		Prices:       &prices,
		MarketCaps:   &marketCap,
		TotalVolumes: &volume,
	}, nil
}

func (h *Handler) PriceTokenAggregator() error {
	// symbolKV[Symbol][Store]=price
	symbolKV := make(map[string]map[string]float64)
	stores := []string{BinanceStore, CoingeckoStore}

	whitelist := make(map[string]struct{})
	cnsWhitelist, err := h.GetCNSWhitelistedTokens()
	if err != nil {
		return fmt.Errorf("GetCNSWhitelistedTokens: %w", err)
	}
	for _, token := range cnsWhitelist {
		baseToken := token + types.USDT
		whitelist[baseToken] = struct{}{}
	}

	for _, s := range stores {
		prices, err := h.Store.GetPrices(s)
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
		mean, err := Averaging(symbolKV[token])
		if err != nil {
			return fmt.Errorf("Store.PriceTokenAggregator: %w", err)
		}

		if err = h.Store.UpsertPrice(TokensStore, mean, token); err != nil {
			return fmt.Errorf("Store.UpsertTokenPrice(%f,%s): %w", mean, token, err)
		}
	}
	return nil
}

func (h *Handler) PriceFiatAggregator() error {
	// symbolKV[Symbol][Store]=price
	symbolKV := make(map[string]map[string]float64)
	stores := []string{FixerStore}

	whitelist := make(map[string]struct{})
	for _, fiat := range h.Cfg.WhitelistedFiats {
		baseFiat := types.USD + fiat
		whitelist[baseFiat] = struct{}{}
	}

	for _, s := range stores {
		prices, err := h.Store.GetPrices(s)
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
		mean, err := Averaging(symbolKV[fiat])
		if err != nil {
			return fmt.Errorf("Store.PriceFiatAggregator: %w", err)
		}

		if err := h.Store.UpsertPrice(FiatsStore, mean, fiat); err != nil {
			return fmt.Errorf("Store.UpsertFiatPrice(%f,%s): %w", mean, fiat, err)
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
