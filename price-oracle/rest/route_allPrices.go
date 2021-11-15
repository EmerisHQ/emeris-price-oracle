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
	var tokens []string
	for _, token := range whitelists {
		tokens = append(tokens, token+types.USDTBasecurrency)
	}

	selectTokens := types.SelectToken{
		Tokens: tokens,
	}
	Tokens, err := r.s.sh.Store.GetTokens(selectTokens)
	if err != nil {
		r.s.l.Error("Error", "Store.GetTokens()", err.Error(), "Duration", time.Second)
		return nil, nil, err
	}

	var fiats []string
	for _, fiat := range r.s.c.Whitelistfiats {
		fiats = append(fiats, types.USDBasecurrency+fiat)
	}
	selectFiats := types.SelectFiat{
		Fiats: fiats,
	}
	Fiats, err := r.s.sh.Store.GetFiats(selectFiats)
	if err != nil {
		r.s.l.Error("Error", "Store.GetFiats()", err.Error(), "Duration", time.Second)
		return Tokens, nil, err
	}

	return Tokens, Fiats, nil
}

func (r *router) allPricesHandler(ctx *gin.Context) {
	var AllPriceResponse types.AllPriceResponse
	if r.s.ri.Exists("prices") {
		bz, err := r.s.ri.Client.Get(context.Background(), "prices").Bytes()
		if err != nil {
			r.s.l.Error("Error", "Redis-Get", err.Error(), "Duration", time.Second)
			goto STORE
		}
		err = json.Unmarshal(bz, &AllPriceResponse)
		if err != nil {
			r.s.l.Error("Error", "Redis-Unmarshal", err.Error(), "Duration", time.Second)
			goto STORE
		}
		ctx.JSON(http.StatusOK, gin.H{
			"status":  http.StatusOK,
			"data":    &AllPriceResponse,
			"message": nil,
		})

		return
	}
STORE:
	Tokens, Fiats, err := allPrices(r)
	if err != nil {
		e(ctx, http.StatusInternalServerError, err)
		return
	}
	AllPriceResponse.Tokens = Tokens
	AllPriceResponse.Fiats = Fiats

	bz, err := json.Marshal(AllPriceResponse)
	if err != nil {
		r.s.l.Error("Error", "Marshal AllPriceResponse", err.Error(), "Duration", time.Second)
		return
	}
	err = r.s.ri.SetWithExpiryTime("prices", string(bz), r.s.c.RedisExpiry)
	if err != nil {
		r.s.l.Error("Error", "Redis-Set", err.Error(), "Duration", time.Second)
		return
	}
	ctx.JSON(http.StatusOK, gin.H{
		"status":  http.StatusOK,
		"data":    &AllPriceResponse,
		"message": nil,
	})
}

func (r *router) getallPrices() (string, gin.HandlerFunc) {
	return getAllPriceRoute, r.allPricesHandler
}
