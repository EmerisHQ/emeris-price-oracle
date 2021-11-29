package store

import (
	"fmt"
	"github.com/allinbits/emeris-price-oracle/price-oracle/config"
	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"go.uber.org/zap"
	"math/rand"
	"sync"

	"time"
)

type Store interface {
	Init() error //runs migrations
	Close() error
	GetTokenPriceAndSupplies(tokens []string) ([]types.TokenPriceAndSupply, error)
	GetFiatPrices(fiats []string) ([]types.FiatPrice, error)
	GetTokenNames() ([]string, error)
	GetPriceIDs() ([]string, error)
	GetPrices(from string) ([]types.Prices, error)
	UpsertPrice(to string, price float64, token string) error
	UpsertToken(to string, symbol string, price float64, time int64) error
	UpsertTokenSupply(to string, symbol string, supply float64) error
}

const (
	BinanceStore         = "oracle.binance"
	CoingeckoStore       = "oracle.coingecko"
	FixerStore           = "oracle.fixer"
	TokensStore          = "oracle.tokens"
	FiatsStore           = "oracle.fiats"
	CoingeckoSupplyStore = "oracle.coingeckosupply"
)

type Handler struct {
	Store  Store
	Logger *zap.SugaredLogger
	Cfg    *config.Config
	Cache  *Cache
}

type Cache struct {
	Whitelist             []string
	TokenPriceAndSupplies map[string]types.TokenPriceAndSupply
	FiatPrices            map[string]types.FiatPrice

	RefreshInterval time.Duration
	Mu              sync.Mutex
}

func NewStoreHandler(store Store, logger *zap.SugaredLogger, cfg *config.Config, cache *Cache) (*Handler, error) {
	if store == nil {
		return nil, fmt.Errorf("store.go, NewStoreHandler : nil store passed")
	}
	if cfg == nil {
		return nil, fmt.Errorf("store.go, NewStoreHandler : nil configuration passed")
	}
	if logger == nil {
		return nil, fmt.Errorf("store.go, NewStoreHandler : nil logger passed")
	}
	if cache == nil {
		cache = &Cache{
			Whitelist:             nil,
			TokenPriceAndSupplies: nil,
			FiatPrices:            nil,
			RefreshInterval:       time.Second * 5,
			Mu:                    sync.Mutex{},
		}
	}

	if err := store.Init(); err != nil {
		return nil, err
	}
	// Invalidate in-memory cache after RefreshInterval
	go func(cache *Cache) {
		randomInt := rand.New(rand.NewSource(time.Now().UnixNano())).Int63n(5) + 5
		d := cache.RefreshInterval + (cache.RefreshInterval / time.Duration(randomInt))
		for {
			select {
			case <-time.Tick(d):
				fmt.Println("Invalidate in-memory cache", time.Now().Second()) // Feeling cute, might delete later! UwU
				cache.Mu.Lock()
				cache.Whitelist = nil
				cache.FiatPrices = nil
				cache.TokenPriceAndSupplies = nil
				cache.Mu.Unlock()
			}
		}
	}(cache)
	return &Handler{Store: store, Logger: logger, Cfg: cfg, Cache: cache}, nil
}

// GetCNSWhitelistedTokens returns the whitelisted tokens.
// It first checks the in-memory cache.
// If cache is nil, it fetches and updates the cache.
func (h *Handler) GetCNSWhitelistedTokens() ([]string, error) {
	if h.Cache.Whitelist == nil {
		whitelists, err := h.Store.GetTokenNames()
		if err != nil {
			return nil, err
		}
		h.Cache.Mu.Lock()
		h.Cache.Whitelist = whitelists
		h.Cache.Mu.Unlock()
	}
	return h.Cache.Whitelist, nil
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
	cachedTokens := make([]string, 0, len(h.Cache.TokenPriceAndSupplies))
	for t := range h.Cache.TokenPriceAndSupplies {
		cachedTokens = append(cachedTokens, t)
	}

	if h.Cache.TokenPriceAndSupplies == nil || !isSubset(tokens, cachedTokens) {
		tokensDetails, err := h.Store.GetTokenPriceAndSupplies(tokens)
		if err != nil {
			return nil, err
		}

		if h.Cache.TokenPriceAndSupplies == nil {
			h.Cache.TokenPriceAndSupplies = make(map[string]types.TokenPriceAndSupply, len(tokensDetails))
		}
		h.Cache.Mu.Lock()
		for _, t := range tokensDetails {
			h.Cache.TokenPriceAndSupplies[t.Symbol] = t
		}
		h.Cache.Mu.Unlock()
		return tokensDetails, err
	}

	var tokenDetails []types.TokenPriceAndSupply
	for _, t := range tokens {
		tokenDetails = append(tokenDetails, h.Cache.TokenPriceAndSupplies[t])
	}
	return tokenDetails, nil
}

// GetFiatPrices returns a list of FiatPrice. It first checks if
// in-memory cache is still valid and all requested tokens are cached.
// If not it fetches all the requested tokens and updates the cache.
func (h *Handler) GetFiatPrices(fiats []string) ([]types.FiatPrice, error) {
	cachedFiats := make([]string, 0, len(h.Cache.FiatPrices))
	for f := range h.Cache.FiatPrices {
		cachedFiats = append(cachedFiats, f)
	}

	if h.Cache.FiatPrices == nil || !isSubset(fiats, cachedFiats) {
		fiatPrices, err := h.Store.GetFiatPrices(fiats)
		if err != nil {
			return nil, err
		}

		if h.Cache.FiatPrices == nil {
			h.Cache.FiatPrices = make(map[string]types.FiatPrice, len(fiatPrices))
		}
		h.Cache.Mu.Lock()
		for _, f := range fiatPrices {
			h.Cache.FiatPrices[f.Symbol] = f
		}
		h.Cache.Mu.Unlock()
		return fiatPrices, nil
	}
	var fiatPrices []types.FiatPrice
	for _, f := range fiats {
		fiatPrices = append(fiatPrices, h.Cache.FiatPrices[f])
	}
	return fiatPrices, nil
}

func (h *Handler) PriceTokenAggregator() error {
	symbolKV := make(map[string][]float64)
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

			//do not update if it was already updated in the last minute
			if token.UpdatedAt < now.Unix()-60 {
				continue
			}
			symbolKV[token.Symbol] = append(symbolKV[token.Symbol], token.Price)
		}
	}

	for token := range whitelist {
		var total float64 = 0
		for _, value := range symbolKV[token] {
			total += value
		}
		if len(symbolKV[token]) == 0 {
			return nil
		}

		mean := total / float64(len(symbolKV[token]))

		if err = h.Store.UpsertPrice(TokensStore, mean, token); err != nil {
			return fmt.Errorf("Store.UpsertTokenPrice(%f,%s): %w", mean, token, err)
		}
	}
	return nil
}

func (h *Handler) PriceFiatAggregator() error {
	symbolKV := make(map[string][]float64)
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
			symbolKV[fiat.Symbol] = append(symbolKV[fiat.Symbol], fiat.Price)
		}
	}
	for fiat := range whitelist {
		var total float64 = 0
		for _, value := range symbolKV[fiat] {
			total += value
		}
		if len(symbolKV[fiat]) == 0 {
			return nil
		}
		mean := total / float64(len(symbolKV[fiat]))

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
