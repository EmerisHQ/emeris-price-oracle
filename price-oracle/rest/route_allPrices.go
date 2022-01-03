package rest

import (
	"net/http"

	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"github.com/gin-gonic/gin"
	_ "github.com/jackc/pgx/v4/stdlib"
)

const getAllPriceRoute = "/prices"

func (r *router) allPricesHandler(ctx *gin.Context) {
	whitelistedTokens, err := r.s.sh.GetCNSWhitelistedTokens()
	if err != nil {
		r.s.l.Errorw("Store.GetCNSWhitelistedTokens()", err.Error())
		e(ctx, http.StatusInternalServerError, err)
		return
	}

	// if token is ATOM; then corresponding token symbol is ATOMUSDT.
	whitelistedTokenSymbols := make([]string, 0, len(whitelistedTokens))
	for _, token := range whitelistedTokens {
		whitelistedTokenSymbols = append(whitelistedTokenSymbols, token+types.USDT)
	}

	tokenPriceAndSupplies, err := r.s.sh.GetTokenPriceAndSupplies(whitelistedTokenSymbols)
	if err != nil {
		r.s.l.Errorw("Store.GetTokenPriceAndSupplies()", err.Error())
		e(ctx, http.StatusInternalServerError, err)
		return
	}

	// if fiat is EUR; then corresponding fiat symbol is USDEUR.
	whitelistedFiatSymbols := make([]string, 0, len(r.s.c.WhitelistedFiats))
	for _, fiat := range r.s.c.WhitelistedFiats {
		whitelistedFiatSymbols = append(whitelistedFiatSymbols, types.USD+fiat)
	}

	fiatPrices, err := r.s.sh.GetFiatPrices(whitelistedFiatSymbols)
	if err != nil {
		r.s.l.Errorw("Store.GetFiatPrices()", err.Error())
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
