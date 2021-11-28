package rest

import (
	"github.com/allinbits/emeris-price-oracle/price-oracle/store"
	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"github.com/gin-gonic/gin"
	_ "github.com/jackc/pgx/v4/stdlib"
	"go.uber.org/zap"
	"net/http"
)

const getFiatsPricesRoute = "/fiats"

func getFiatPrices(
	fiats []string,
	whitelisted []string,
	store *store.Handler,
	logger *zap.SugaredLogger) ([]types.FiatPrice, int, error) {

	var fiatSymbols []string
	for _, fiat := range whitelisted {
		fiatSymbols = append(fiatSymbols, types.USD+fiat)
	}

	if !isSubset(fiats, fiatSymbols) {
		return nil, http.StatusForbidden, errNotWhitelistedAsset
	}

	fiatPrices, err := store.GetFiatPrices(fiats)
	if err != nil {
		logger.Error("Error", "Store.GetFiatPrices()", err.Error())
		return nil, http.StatusInternalServerError, err
	}
	return fiatPrices, http.StatusOK, nil
}

func (r *router) fiatPriceHandler(ctx *gin.Context) {
	var fiats types.Fiats
	if err := ctx.BindJSON(&fiats); err != nil {
		r.s.l.Error("Error", "FiatPrices", err.Error())
		e(ctx, http.StatusBadRequest, err)
		return
	}

	if fiats.Fiats == nil || len(fiats.Fiats) == 0 || len(fiats.Fiats) > 10 {
		err := errZeroAsset
		if len(fiats.Fiats) > 10 {
			err = errAssetLimitExceed
		} else if fiats.Fiats == nil {
			err = errNilAsset
		}
		e(ctx, http.StatusForbidden, err)
		return
	}

	fiatPrices, httpStatus, err := getFiatPrices(fiats.Fiats, r.s.c.WhitelistFiats, r.s.sh, r.s.l)
	if err != nil {
		e(ctx, httpStatus, err)
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"status":  http.StatusOK,
		"data":    &fiatPrices,
		"message": nil,
	})
}

func (r *router) getFiatsPrices() (string, gin.HandlerFunc) {
	return getFiatsPricesRoute, r.fiatPriceHandler
}
