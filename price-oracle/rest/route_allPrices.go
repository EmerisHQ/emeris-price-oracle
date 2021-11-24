package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"github.com/gin-gonic/gin"
	_ "github.com/jackc/pgx/v4/stdlib"
)

const getAllPriceRoute = "/prices"

func allPrices(r *router) ([]types.TokenPriceResponse, []types.FiatPriceResponse, error) {
	whitelists, err := r.s.sh.CnsTokenQuery()
	if err != nil {
		r.s.l.Error("Error", "CnsTokenQuery()", err.Error(), "Duration", time.Second)
		return nil, nil, err
	}
	var tokensWhitelist []string
	for _, token := range whitelists {
		tokensWhitelist = append(tokensWhitelist, token+types.USDTBasecurrency)
	}

	selectTokens := types.SelectToken{
		Tokens: tokensWhitelist,
	}
	tokens, err := r.s.sh.Store.GetTokens(selectTokens)
	if err != nil {
		r.s.l.Error("Error", "Store.GetTokens()", err.Error(), "Duration", time.Second)
		return nil, nil, err
	}

	var fiatsWhitelist []string
	for _, fiat := range r.s.c.Whitelistfiats {
		fiatsWhitelist = append(fiatsWhitelist, types.USDBasecurrency+fiat)
	}
	selectFiats := types.SelectFiat{
		Fiats: fiatsWhitelist,
	}
	fiats, err := r.s.sh.Store.GetFiats(selectFiats)
	if err != nil {
		r.s.l.Error("Error", "Store.GetFiats()", err.Error(), "Duration", time.Second)
		return tokens, nil, err
	}

	return tokens, fiats, nil
}

func (r *router) allPricesHandler(ctx *gin.Context) {
	var AllPriceResponse types.AllPriceResponse
	if r.s.ri.Exists("prices") {
		bz, err := r.s.ri.Client.Get(context.Background(), "prices").Bytes()
		if err != nil {
			r.s.l.Error("Error", "Redis-Get", err.Error(), "Duration", time.Second)
			fetchAllPricesFromStore(r, ctx)
			return
		}

		if err = json.Unmarshal(bz, &AllPriceResponse); err != nil {
			r.s.l.Error("Error", "Redis-Unmarshal", err.Error(), "Duration", time.Second)
			fetchAllPricesFromStore(r, ctx)
			return
		}
		ctx.JSON(http.StatusOK, gin.H{
			"status":  http.StatusOK,
			"data":    &AllPriceResponse,
			"message": nil,
		})

		return
	}
	fetchAllPricesFromStore(r, ctx)
}

func (r *router) getAllPrices() (string, gin.HandlerFunc) {
	return getAllPriceRoute, r.allPricesHandler
}

func fetchAllPricesFromStore(r *router, ctx *gin.Context) {
	var AllPriceResponse types.AllPriceResponse
	tokens, fiats, err := allPrices(r)
	if err != nil {
		e(ctx, http.StatusInternalServerError, err)
		return
	}
	AllPriceResponse.Tokens = tokens
	AllPriceResponse.Fiats = fiats

	bz, err := json.Marshal(AllPriceResponse)
	if err != nil {
		r.s.l.Error("Error", "Marshal AllPriceResponse", err.Error(), "Duration", time.Second)
		return
	}
	if err := r.s.ri.SetWithExpiryTime("prices", string(bz), r.s.c.RedisExpiry); err != nil {
		r.s.l.Error("Error", "Redis-Set", err.Error(), "Duration", time.Second)
		return
	}
	ctx.JSON(http.StatusOK, gin.H{
		"status":  http.StatusOK,
		"data":    &AllPriceResponse,
		"message": nil,
	})
}
