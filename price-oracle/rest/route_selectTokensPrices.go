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

const getselectTokensPricesRoute = "/tokens"

func (r *router) TokensPrices(ctx *gin.Context) {
	var selectToken types.SelectToken
	var symbols []types.TokenPriceResponse

	err := ctx.BindJSON(&selectToken)
	if err != nil {
		r.s.l.Error("Error", "TokensPrices", err.Error(), "Duration", time.Second)
	}
	if len(selectToken.Tokens) > 10 {
		ctx.JSON(http.StatusForbidden, gin.H{
			"status":  http.StatusForbidden,
			"data":    nil,
			"message": "Not allow More than 10 asset",
		})
		return
	}

	if selectToken.Tokens == nil {
		ctx.JSON(http.StatusForbidden, gin.H{
			"status":  http.StatusForbidden,
			"data":    nil,
			"message": "Not allow nil asset",
		})
		return
	}

	if len(selectToken.Tokens) == 0 {
		ctx.JSON(http.StatusForbidden, gin.H{
			"status":  http.StatusForbidden,
			"data":    nil,
			"message": "Not allow 0 asset",
		})
		return
	}

	whitelists, err := r.s.sh.CnsTokenQuery()
	if err != nil {
		r.s.l.Error("Error", "DB", err.Error(), "Duration", time.Second)
		return
	}
	var basetokens []string
	for _, token := range whitelists {
		tokens := token + types.USDTBasecurrency
		basetokens = append(basetokens, tokens)
	}
	if !Diffpair(selectToken.Tokens, basetokens) {
		ctx.JSON(http.StatusForbidden, gin.H{
			"status":  http.StatusForbidden,
			"data":    nil,
			"message": "Not whitelisting asset",
		})
		return
	}
	selectTokenkey, err := json.Marshal(selectToken.Tokens)
	if err != nil {
		r.s.l.Error("Error", "Redis-selectTokenkey", err.Error(), "Duration", time.Second)
		return
	}
	if r.s.ri.Exists(string(selectTokenkey)) {
		bz, err := r.s.ri.Client.Get(context.Background(), string(selectTokenkey)).Bytes()
		if err != nil {
			r.s.l.Error("Error", "Redis-Get", err.Error(), "Duration", time.Second)
			return
		}
		err = json.Unmarshal(bz, &symbols)
		if err != nil {
			r.s.l.Error("Error", "Redis-Unmarshal", err.Error(), "Duration", time.Second)
			return
		}
		ctx.JSON(http.StatusOK, gin.H{
			"status":  http.StatusOK,
			"data":    &symbols,
			"message": nil,
		})

		return
	}
	symbols, err = r.s.sh.Store.GetTokens(selectToken)
	if err != nil {
		e(ctx, http.StatusInternalServerError, err)
		r.s.l.Error("Error", "Store.GetTokens()", err.Error(), "Duration", time.Second)
		return
	}
	bz, err := json.Marshal(symbols)
	if err != nil {
		r.s.l.Error("Error", "Marshal symbols", err.Error(), "Duration", time.Second)
		return
	}
	err = r.s.ri.SetWithExpiryTime(string(selectTokenkey), string(bz), r.s.c.RedisExpiry)
	if err != nil {
		r.s.l.Error("Error", "Redis-Set", err.Error(), "Duration", time.Second)
		return
	}
	ctx.JSON(http.StatusOK, gin.H{
		"status":  http.StatusOK,
		"data":    &symbols,
		"message": nil,
	})
}

func (r *router) getselectTokensPrices() (string, gin.HandlerFunc) {
	return getselectTokensPricesRoute, r.TokensPrices
}
