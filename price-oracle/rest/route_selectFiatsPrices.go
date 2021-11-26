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
	fiats types.Fiats,
	whitelisted []string,
	store *store.Handler,
	logger *zap.SugaredLogger) ([]types.FiatPrice, int, error) {

	var fiatSymbols []string
	for _, fiat := range whitelisted {
		fiatSymbols = append(fiatSymbols, types.USD+fiat)
	}

	if !IsSubset(fiats.Fiats, fiatSymbols) {
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
	var selectFiat types.Fiats
	if err := ctx.BindJSON(&selectFiat); err != nil {
		r.s.l.Error("Error", "FiatPrices", err.Error())
		e(ctx, http.StatusBadRequest, err)
		return
	}

	if selectFiat.Fiats == nil || len(selectFiat.Fiats) == 0 || len(selectFiat.Fiats) > 10 {
		err := errZeroAsset
		if len(selectFiat.Fiats) > 10 {
			err = errAssetLimitExceed
		} else if selectFiat.Fiats == nil {
			err = errNilAsset
		}
		e(ctx, http.StatusForbidden, err)
		return
	}

	fiatPrices, httpStatus, err := getFiatPrices(selectFiat, r.s.c.Whitelistfiats, r.s.sh, r.s.l)
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
