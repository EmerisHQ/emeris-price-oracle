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

const getTokensPricesRoute = "/tokens"

func getTokenPriceAndSupplies(
	ctx context.Context,
	tokens []string,
	store *store.Handler,
	logger *zap.SugaredLogger) ([]types.TokenPriceAndSupply, int, error) {

	whitelistedTokens, err := store.GetCNSWhitelistedTokens(ctx)
	if err != nil {
		logger.Errorw("store.GetCNSWhitelistedTokens()", "error", err.Error())
		return nil, http.StatusInternalServerError, err
	}
	whitelistedTokenSymbols := make([]string, 0, len(whitelistedTokens))
	for _, token := range whitelistedTokens {
		whitelistedTokenSymbols = append(whitelistedTokenSymbols, token+types.USDT)
	}

	if !isSubset(tokens, whitelistedTokenSymbols) {
		return nil, http.StatusForbidden, errNotWhitelistedAsset
	}

	tokenPriceAndSupplies, err := store.GetTokenPriceAndSupplies(ctx, tokens)
	if err != nil {
		logger.Errorw("Store.GetTokenPriceAndSupplies()", "error", err.Error())
		return nil, http.StatusInternalServerError, err
	}
	return tokenPriceAndSupplies, http.StatusOK, nil
}

func (r *router) tokensPriceAndSuppliesHandler(ctx *gin.Context) {
	var tokens types.Tokens
	if err := ctx.BindJSON(&tokens); err != nil {
		r.s.l.Errorw("TokenPriceAndSupplies", "error", err.Error())
		e(ctx, http.StatusBadRequest, err)
		return
	}

	if len(tokens.Tokens) == 0 || len(tokens.Tokens) > r.s.c.MaxAssetsReq {
		err := errZeroAsset
		if len(tokens.Tokens) > r.s.c.MaxAssetsReq {
			err = errAssetLimitExceed
		} else if tokens.Tokens == nil {
			err = errNilAsset
		}
		e(ctx, http.StatusForbidden, err)
		return
	}

	tokenPriceAndSupplies, httpStatus, err := getTokenPriceAndSupplies(ctx.Request.Context(), tokens.Tokens, r.s.sh, r.s.l)
	if err != nil {
		e(ctx, httpStatus, err)
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"status":  http.StatusOK,
		"data":    &tokenPriceAndSupplies,
		"message": nil,
	})
}

func (r *router) getTokensPriceAndSupplies() (string, gin.HandlerFunc) {
	return getTokensPricesRoute, r.tokensPriceAndSuppliesHandler
}
