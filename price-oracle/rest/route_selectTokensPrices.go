package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/allinbits/emeris-price-oracle/price-oracle/types"
	"github.com/gin-gonic/gin"
	_ "github.com/jackc/pgx/v4/stdlib"
)

const getselectTokensPricesRoute = "/tokens"

type selectTokensResp struct {
	Status  int                         `json:"status"`
	Data    *[]types.TokenPriceResponse `json:"data"`
	Message string                      `json:"message"`
}

func selectTokensPrices(r *router, selectToken types.SelectToken) ([]types.TokenPriceResponse, error) {
	var Tokens []types.TokenPriceResponse
	var Token types.TokenPriceResponse
	var symbolList []interface{}

	symbolNum := len(selectToken.Tokens)

	query := "SELECT * FROM oracle.tokens WHERE symbol=$1"

	for i := 2; i <= symbolNum; i++ {
		query += " OR" + " symbol=$" + strconv.Itoa(i)
	}

	for _, usersymbol := range selectToken.Tokens {
		symbolList = append(symbolList, usersymbol)
	}

	rows, err := r.s.d.Query(query, symbolList...)
	if err != nil {
		r.s.l.Error("Error", "DB", err.Error(), "Duration", time.Second)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var symbol string
		var price float64
		var supply float64
		err := rows.Scan(&symbol, &price)
		if err != nil {
			r.s.l.Error("Error", "DB", err.Error(), "Duration", time.Second)
			return nil, err
		}
		//rowCmcSupply, err := r.s.d.Query("SELECT * FROM oracle.coinmarketcapsupply WHERE symbol=$1", symbol)
		rowCmcSupply, err := r.s.d.Query("SELECT * FROM oracle.coingeckosupply WHERE symbol=$1", symbol)
		if err != nil {
			r.s.l.Error("Error", "DB", err.Error(), "Duration", time.Second)
			return nil, err
		}
		defer rowCmcSupply.Close()
		for rowCmcSupply.Next() {
			if err := rowCmcSupply.Scan(&symbol, &supply); err != nil {
				r.s.l.Error("Error", "DB", err.Error(), "Duration", time.Second)
			}
		}
		Token.Symbol = symbol
		Token.Price = price
		Token.Supply = supply

		Tokens = append(Tokens, Token)
	}

	return Tokens, nil
}

// @Summary Return a list of token prices.
// @Description get the requested list of token prices. The method supports up to 10 prices per call.
// @Router /tokens [post]
// @Param tokenList body types.SelectToken true "List of token names to return prices for"
// @Produce json
// @Success 200 {object} selectTokensResp
// @Failure 403 "if requesting 0 token prices, more than 10 token prices or a token which is not whitelisted"
// @Failure 500 "on error"
func (r *router) TokensPrices(ctx *gin.Context) {
	var selectToken types.SelectToken
	var symbols []types.TokenPriceResponse

	err := ctx.BindJSON(&selectToken)
	if err != nil {
		r.s.l.Error("Error", "TokensPrices", err.Error(), "Duration", time.Second)
	}
	if len(selectToken.Tokens) > 10 {
		ctx.JSON(http.StatusForbidden, selectTokensResp{
			Status:  http.StatusForbidden,
			Data:    nil,
			Message: "Not allow more than 10 asset",
		})
		return
	}

	if selectToken.Tokens == nil {
		ctx.JSON(http.StatusForbidden, selectTokensResp{
			Status:  http.StatusForbidden,
			Data:    nil,
			Message: "Not allow nil asset",
		})
		return
	}

	if len(selectToken.Tokens) == 0 {
		ctx.JSON(http.StatusForbidden, selectTokensResp{
			Status:  http.StatusForbidden,
			Data:    nil,
			Message: "Not allow 0 asset",
		})
		return
	}

	Whitelists, err := r.s.d.CnstokenQueryHandler()
	if err != nil {
		r.s.l.Error("Error", "DB", err.Error(), "Duration", time.Second)
		return
	}
	var basetokens []string
	for _, token := range Whitelists {
		tokens := token + types.USDTBasecurrency
		basetokens = append(basetokens, tokens)
	}
	if Diffpair(selectToken.Tokens, basetokens) == false {
		ctx.JSON(http.StatusForbidden, selectTokensResp{
			Status:  http.StatusForbidden,
			Data:    nil,
			Message: "Not whitelisted asset",
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
		ctx.JSON(http.StatusOK, selectTokensResp{
			Status:  http.StatusOK,
			Data:    &symbols,
			Message: "",
		})

		return
	}
	symbols, err = selectTokensPrices(r, selectToken)
	if err != nil {
		e(ctx, http.StatusInternalServerError, err)
		return
	}
	bz, err := json.Marshal(symbols)
	err = r.s.ri.SetWithExpiryTime(string(selectTokenkey), string(bz), 10*time.Second)
	if err != nil {
		r.s.l.Error("Error", "Redis-Set", err.Error(), "Duration", time.Second)
		return
	}
	ctx.JSON(http.StatusOK, selectTokensResp{
		Status:  http.StatusOK,
		Data:    &symbols,
		Message: "",
	})
}

func (r *router) getselectTokensPrices() (string, gin.HandlerFunc) {
	return getselectTokensPricesRoute, r.TokensPrices
}
