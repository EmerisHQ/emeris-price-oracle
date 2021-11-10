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

	Whitelists, err := r.s.sh.CnsTokenQuery()
	if err != nil {
		r.s.l.Error("Error", "CnsTokenQuery()", err.Error(), "Duration", time.Second)
		return nil, nil, err
	}

	selectTokens := types.SelectToken{
		Tokens: Whitelists,
	}
	Tokens, err := r.s.sh.Store.GetTokens(selectTokens)
	if err != nil {
		r.s.l.Error("Error", "Store.GetTokens()", err.Error(), "Duration", time.Second)
		return nil, nil, err
	}

	selectFiats := types.SelectFiat{
		Fiats: r.s.c.Whitelistfiats,
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
			return
		}
		err = json.Unmarshal(bz, &AllPriceResponse)
		if err != nil {
			r.s.l.Error("Error", "Redis-Unmarshal", err.Error(), "Duration", time.Second)
			return
		}
		ctx.JSON(http.StatusOK, gin.H{
			"status":  http.StatusOK,
			"data":    &AllPriceResponse,
			"message": nil,
		})

		return
	}
	Tokens, Fiats, err := allPrices(r)
	AllPriceResponse.Tokens = Tokens
	AllPriceResponse.Fiats = Fiats
	if err != nil {
		e(ctx, http.StatusInternalServerError, err)
		return
	}
	bz, err := json.Marshal(AllPriceResponse)
	if err != nil {
		r.s.l.Error("Error", "Marshal AllPriceResponse", err.Error(), "Duration", time.Second)
		return
	}
	err = r.s.ri.SetWithExpiryTime("prices", string(bz), 10*time.Second)
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
