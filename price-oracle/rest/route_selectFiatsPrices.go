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

func (r *router) FiatsPrices(ctx *gin.Context) {
	var selectFiat types.SelectFiat
	var symbols []types.FiatPriceResponse

	if err := ctx.BindJSON(&selectFiat); err != nil {
		r.s.l.Error("Error", "FiatsPrices", err.Error())
	}

	if len(selectFiat.Fiats) > r.s.c.MaxAssetsReq {
		ctx.JSON(http.StatusForbidden, gin.H{
			"status":  http.StatusForbidden,
			"data":    nil,
			"message": "Not allow More than " + strconv.Itoa(r.s.c.MaxAssetsReq) + " asset",
		})
		return
	}

	if selectFiat.Fiats == nil {
		ctx.JSON(http.StatusForbidden, gin.H{
			"status":  http.StatusForbidden,
			"data":    nil,
			"message": "Not allow nil asset",
		})
		return
	}

	if len(selectFiat.Fiats) == 0 {
		ctx.JSON(http.StatusForbidden, gin.H{
			"status":  http.StatusForbidden,
			"data":    nil,
			"message": "Not allow 0 asset",
		})
		return
	}

	var basefiats []string
	for _, fiat := range r.s.c.Whitelistfiats {
		fiats := types.USDBasecurrency + fiat
		basefiats = append(basefiats, fiats)
	}
	if !IsSubset(selectFiat.Fiats, basefiats) {
		ctx.JSON(http.StatusForbidden, gin.H{
			"status":  http.StatusForbidden,
			"data":    nil,
			"message": "Not whitelisting asset",
		})
		return
	}
	selectFiatkey, err := json.Marshal(selectFiat.Fiats)
	if err != nil {
		r.s.l.Error("Error", "Redis-selectFiatkey", err.Error())
		return
	}
	if r.s.ri.Exists(string(selectFiatkey)) {
		bz, err := r.s.ri.Client.Get(context.Background(), string(selectFiatkey)).Bytes()
		if err != nil {
			r.s.l.Error("Error", "Redis-Get", err.Error())
			fetchFiatPricesFromStore(r, ctx, selectFiat, selectFiatkey)
			return
		}

		if err = json.Unmarshal(bz, &symbols); err != nil {
			r.s.l.Error("Error", "Redis-Unmarshal", err.Error())
			fetchFiatPricesFromStore(r, ctx, selectFiat, selectFiatkey)
			return
		}
		ctx.JSON(http.StatusOK, gin.H{
			"status":  http.StatusOK,
			"data":    &symbols,
			"message": nil,
		})

		return
	}
	fetchFiatPricesFromStore(r, ctx, selectFiat, selectFiatkey)
}

func (r *router) getselectFiatsPrices() (string, gin.HandlerFunc) {
	return getselectFiatsPricesRoute, r.FiatsPrices
}

func fetchFiatPricesFromStore(r *router, ctx *gin.Context, selectFiat types.SelectFiat, selectFiatkey []byte) {
	symbols, err := r.s.sh.Store.GetFiats(selectFiat)
	if err != nil {
		r.s.l.Error("Error", "Store.GetFiats()", err.Error(), "Duration", time.Second)
	}
	bz, err := json.Marshal(symbols)
	if err != nil {
		r.s.l.Error("Error", "Marshal symbols", err.Error(), "Duration", time.Second)
		return
	}

	if err = r.s.ri.SetWithExpiryTime(string(selectFiatkey), string(bz), r.s.c.RedisExpiry); err != nil {
		r.s.l.Error("Error", "Redis-Set", err.Error(), "Duration", time.Second)
		return
	}
	ctx.JSON(http.StatusOK, gin.H{
		"status":  http.StatusOK,
		"data":    &symbols,
		"message": nil,
	})
}
