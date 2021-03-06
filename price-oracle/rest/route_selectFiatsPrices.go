package rest

import (
	"context"
	"net/http"

	"github.com/emerishq/emeris-price-oracle/price-oracle/store"
	"github.com/emerishq/emeris-price-oracle/price-oracle/types"
	"github.com/gin-gonic/gin"
	_ "github.com/jackc/pgx/v4/stdlib"
	"go.uber.org/zap"
)

const getFiatsPricesRoute = "/fiats"

func getFiatPrices(
	ctx context.Context,
	fiats []string,
	whitelisted []string,
	store *store.Handler,
	logger *zap.SugaredLogger) ([]types.FiatPrice, int, error) {

	fiatSymbols := make([]string, 0, len(whitelisted))
	for _, fiat := range whitelisted {
		fiatSymbols = append(fiatSymbols, types.USD+fiat)
	}

	if !isSubset(fiats, fiatSymbols) {
		return nil, http.StatusForbidden, errNotWhitelistedAsset
	}

	fiatPrices, err := store.GetFiatPrices(ctx, fiats)
	if err != nil {
		logger.Errorw("Store.GetFiatPrices()", "error", err.Error())
		return nil, http.StatusInternalServerError, err
	}
	return fiatPrices, http.StatusOK, nil
}

func (r *router) fiatPriceHandler(ctx *gin.Context) {
	var fiats types.Fiats
	if err := ctx.BindJSON(&fiats); err != nil {
		r.s.l.Errorw("FiatPrices", "error", err.Error())
		e(ctx, http.StatusBadRequest, err)
		return
	}

	if len(fiats.Fiats) == 0 || len(fiats.Fiats) > r.s.c.MaxAssetsReq {
		err := errZeroAsset
		if len(fiats.Fiats) > r.s.c.MaxAssetsReq {
			err = errAssetLimitExceed
		} else if fiats.Fiats == nil {
			err = errNilAsset
		}
		e(ctx, http.StatusForbidden, err)
		return
	}

	fiatPrices, httpStatus, err := getFiatPrices(ctx.Request.Context(), fiats.Fiats, r.s.c.WhitelistedFiats, r.s.sh, r.s.l)
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
