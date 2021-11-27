package rest

import (
	"github.com/allinbits/emeris-price-oracle/price-oracle/store"
	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"github.com/gin-gonic/gin"
	_ "github.com/jackc/pgx/v4/stdlib"
	"go.uber.org/zap"
	"net/http"
)

const getTokensPricesRoute = "/tokens"

func getTokenPriceAndSupplies(
	tokens []string,
	store *store.Handler,
	logger *zap.SugaredLogger) ([]types.TokenPriceAndSupply, int, error) {

	whitelistedTokens, err := store.GetCNSWhitelistedTokens()
	if err != nil {
		logger.Error("Error", "DB", err.Error())
		return nil, http.StatusInternalServerError, err
	}
	var whitelistedTokenSymbols []string
	for _, token := range whitelistedTokens {
		tokens := token + types.USDT
		whitelistedTokenSymbols = append(whitelistedTokenSymbols, tokens)
	}

	if !isSubset(tokens, whitelistedTokenSymbols) {
		return nil, http.StatusForbidden, errNotWhitelistedAsset
	}

	tokenPriceAndSupplies, err := store.GetTokenPriceAndSupplies(tokens)
	if err != nil {
		logger.Error("Error", "Store.GetTokenPriceAndSupplies()", err.Error())
		return nil, http.StatusInternalServerError, err
	}
	return tokenPriceAndSupplies, http.StatusOK, nil
}

func (r *router) TokensPrices(ctx *gin.Context) {
	var tokens types.Tokens
	if err := ctx.BindJSON(&tokens); err != nil {
		r.s.l.Error("Error", "TokenPrices", err.Error())
		e(ctx, http.StatusBadRequest, err)
		return
	}

	if tokens.Tokens == nil || len(tokens.Tokens) == 0 || len(tokens.Tokens) > 10 {
		err := errZeroAsset
		if len(tokens.Tokens) > 10 {
			err = errAssetLimitExceed
		} else if tokens.Tokens == nil {
			err = errNilAsset
		}
		e(ctx, http.StatusForbidden, err)
		return
	}

	tokenPriceAndSupplies, httpStatus, err := getTokenPriceAndSupplies(tokens.Tokens, r.s.sh, r.s.l)
	if err != nil {
		r.s.l.Error("Error", "Store.GetTokenPriceAndSupplies()", err.Error())
		e(ctx, httpStatus, err)
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"status":  http.StatusOK,
		"data":    &tokenPriceAndSupplies,
		"message": nil,
	})
}

func (r *router) getTokensPrices() (string, gin.HandlerFunc) {
	return getTokensPricesRoute, r.TokensPrices
}
