package rest

import (
	"net/http"
	"time"

	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"github.com/gin-gonic/gin"
	_ "github.com/jackc/pgx/v4/stdlib"
)

const getAllPriceRoute = "/prices"

func (r *router) allPricesHandler(ctx *gin.Context) {
	whitelistedTokens, err := r.s.sh.GetCNSWhitelistedTokens()
	if err != nil {
		r.s.l.Error("Error", "Store.GetCNSWhitelistedTokens()", err.Error())
		e(ctx, http.StatusInternalServerError, err)
		return
	}
	var whitelistedTokenSymbols []string
	for _, token := range whitelistedTokens {
		whitelistedTokenSymbols = append(whitelistedTokenSymbols, token+types.USDT)
	}

	selectTokens := types.Tokens{
		Tokens: whitelistedTokenSymbols,
	}
	tokenPriceAndSupplies, err := r.s.sh.GetTokenPriceAndSupplies(selectTokens)
	if err != nil {
		r.s.l.Error("Error", "Store.GetTokenPriceAndSupplies()", err.Error())
		e(ctx, http.StatusInternalServerError, err)
		return
	}

	var whitelistedFiatSymbols []string
	for _, fiat := range r.s.c.Whitelistfiats {
		whitelistedFiatSymbols = append(whitelistedFiatSymbols, types.USD+fiat)
	}
	selectFiats := types.Fiats{
		Fiats: whitelistedFiatSymbols,
	}
	fiatPrices, err := r.s.sh.GetFiatPrices(selectFiats)
	if err != nil {
		r.s.l.Error("Error", "Store.GetFiatPrices()", err.Error(), "Duration", time.Second)
		e(ctx, http.StatusInternalServerError, err)
		return
	}
	ctx.JSON(http.StatusOK, gin.H{
		"status": http.StatusOK,
		"data": types.AllPriceResponse{
			Fiats:  fiatPrices,
			Tokens: tokenPriceAndSupplies,
		},
		"message": nil,
	})
}

func (r *router) getAllPrices() (string, gin.HandlerFunc) {
	return getAllPriceRoute, r.allPricesHandler
}
