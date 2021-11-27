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

const getselectFiatsPricesRoute = "/fiats"

type selectFiatsResp struct {
	Status  int                        `json:"status"`
	Data    *[]types.FiatPriceResponse `json:"data"`
	Message string                     `json:"message"`
}

func selectFiatsPrices(r *router, selectFiat types.SelectFiat) ([]types.FiatPriceResponse, error) {
	var symbols []types.FiatPriceResponse
	var symbol types.FiatPriceResponse
	var symbolList []interface{}

	symbolNum := len(selectFiat.Fiats)

	query := "SELECT * FROM oracle.fiats WHERE symbol=$1"

	for i := 2; i <= symbolNum; i++ {
		query += " OR" + " symbol=$" + strconv.Itoa(i)
	}

	for _, usersymbol := range selectFiat.Fiats {
		symbolList = append(symbolList, usersymbol)
	}

	rows, err := r.s.d.Query(query, symbolList...)
	if err != nil {
		r.s.l.Error("Error", "DB", err.Error(), "Duration", time.Second)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.StructScan(&symbol)
		if err != nil {
			r.s.l.Error("Error", "DB", err.Error(), "Duration", time.Second)
			return nil, err
		}
		symbols = append(symbols, symbol)
	}

	return symbols, nil
}

// @Summary Return a list of fiat prices.
// @Description get the requested list of fiat prices. The method supports up to 10 prices per call.
// @Router /fiats [post]
// @Param fiatList body types.SelectFiat true "List of fiat names to return prices for"
// @Produce json
// @Success 200 {object} selectFiatsResp
// @Failure 403 "if requesting 0 fiat prices, more than 10 fiat prices or a fiat which is not whitelisted"
// @Failure 500 "on error"
func (r *router) FiatsPrices(ctx *gin.Context) {
	var selectFiat types.SelectFiat
	var symbols []types.FiatPriceResponse

	err := ctx.BindJSON(&selectFiat)
	if err != nil {
		r.s.l.Error("Error", "FiatsPrices", err.Error(), "Duration", time.Second)
	}

	if len(selectFiat.Fiats) > 10 {
		ctx.JSON(http.StatusForbidden, selectFiatsResp{
			Status:  http.StatusForbidden,
			Data:    nil,
			Message: "Not allow more than 10 asset",
		})
		return
	}

	if selectFiat.Fiats == nil {
		ctx.JSON(http.StatusForbidden, selectFiatsResp{
			Status:  http.StatusForbidden,
			Data:    nil,
			Message: "Not allow nil asset",
		})
		return
	}

	if len(selectFiat.Fiats) == 0 {
		ctx.JSON(http.StatusForbidden, selectFiatsResp{
			Status:  http.StatusForbidden,
			Data:    nil,
			Message: "Not allow 0 asset",
		})
		return
	}

	var basefiats []string
	for _, fiat := range r.s.c.Whitelistfiats {
		fiats := types.USDBasecurrency + fiat
		basefiats = append(basefiats, fiats)
	}
	if Diffpair(selectFiat.Fiats, basefiats) == false {
		ctx.JSON(http.StatusForbidden, selectFiatsResp{
			Status:  http.StatusForbidden,
			Data:    nil,
			Message: "Not whitelisted asset",
		})
		return
	}
	selectFiatkey, err := json.Marshal(selectFiat.Fiats)
	if err != nil {
		r.s.l.Error("Error", "Redis-selectFiatkey", err.Error(), "Duration", time.Second)
		return
	}
	if r.s.ri.Exists(string(selectFiatkey)) {
		bz, err := r.s.ri.Client.Get(context.Background(), string(selectFiatkey)).Bytes()
		if err != nil {
			r.s.l.Error("Error", "Redis-Get", err.Error(), "Duration", time.Second)
			return
		}
		err = json.Unmarshal(bz, &symbols)
		if err != nil {
			r.s.l.Error("Error", "Redis-Unmarshal", err.Error(), "Duration", time.Second)
			return
		}
		ctx.JSON(http.StatusOK, selectFiatsResp{
			Status:  http.StatusOK,
			Data:    &symbols,
			Message: "",
		})

		return
	}
	symbols, err = selectFiatsPrices(r, selectFiat)
	if err != nil {
		r.s.l.Error("Error", "SelectFiatQuery", err.Error(), "Duration", time.Second)
	}
	bz, err := json.Marshal(symbols)
	err = r.s.ri.SetWithExpiryTime(string(selectFiatkey), string(bz), 10*time.Second)
	if err != nil {
		r.s.l.Error("Error", "Redis-Set", err.Error(), "Duration", time.Second)
		return
	}
	ctx.JSON(http.StatusOK, selectFiatsResp{
		Status:  http.StatusOK,
		Data:    &symbols,
		Message: "",
	})
}

func (r *router) getselectFiatsPrices() (string, gin.HandlerFunc) {
	return getselectFiatsPricesRoute, r.FiatsPrices
}
