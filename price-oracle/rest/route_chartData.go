package rest

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

const getChartData = "/chart/:id"

var validDays = map[string]bool{"1": true, "7": true, "14": true, "30": true, "90": true, "365": true, "max": true}

func (r *router) chartDataHandler(ctx *gin.Context) {
	var reqQueries struct {
		Days     string `form:"days"`
		Currency string `form:"vs_currency"`
	}
	if err := ctx.ShouldBindQuery(&reqQueries); err != nil {
		r.s.l.Error("Error", "Invalid request query:", err)
		e(ctx, http.StatusBadRequest, fmt.Errorf("invalid request query"))
		return
	}

	if _, ok := validDays[reqQueries.Days]; !ok {
		r.s.l.Error("Error", "Invalid request query:", reqQueries.Days)
		e(ctx, http.StatusBadRequest, fmt.Errorf("invalid request query"))
		return
	}

	vsCurrency := strings.ToLower(reqQueries.Currency)
	if vsCurrency == "" {
		vsCurrency = "usd"
	}
	ok := false
	for _, fiat := range r.s.c.WhitelistedFiats {
		if strings.EqualFold(fiat, vsCurrency) {
			ok = true
			break
		}
	}
	if !ok {
		r.s.l.Error("Error", "Invalid request query:", vsCurrency)
		e(ctx, http.StatusBadRequest, fmt.Errorf("invalid request query"))
		return
	}

	whitelistedPriceIds, err := r.s.sh.CNSPriceIdQuery()
	if err != nil {
		r.s.l.Error("Error", "Store.CNSPriceIdQuery()", err)
		e(ctx, http.StatusInternalServerError, err)
		return
	}

	ok = false
	coinId := ctx.Param("id")
	for _, id := range whitelistedPriceIds {
		if strings.EqualFold(id, coinId) {
			ok = true
			break
		}
	}
	if !ok {
		r.s.l.Error("Error", "Invalid request param:", coinId)
		e(ctx, http.StatusBadRequest, fmt.Errorf("invalid request param"))
		return
	}

	chartData, err := r.s.sh.GetChartData(coinId, reqQueries.Days, reqQueries.Currency, nil)
	if err != nil {
		r.s.l.Error("Error", "Store.GetChartData()", err)
		e(ctx, http.StatusInternalServerError, err)
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"status":  http.StatusOK,
		"data":    chartData,
		"message": nil,
	})
}

func (r *router) getChartData() (string, gin.HandlerFunc) {
	return getChartData, r.chartDataHandler
}
