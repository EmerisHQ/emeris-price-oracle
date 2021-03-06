package rest

import (
	"fmt"
	"net/http"
	"strings"

	gecko "github.com/superoo7/go-gecko/v3"

	"github.com/gin-gonic/gin"
)

const getChartData = "/chart/:id"

var validDays = map[string]struct{}{"1": {}, "7": {}, "14": {}, "30": {}, "90": {}, "365": {}, "max": {}}

func (r *router) chartDataHandler(ctx *gin.Context) {
	var reqQueries struct {
		Days     string `form:"days"`
		Currency string `form:"vs_currency"`
	}
	if err := ctx.ShouldBindQuery(&reqQueries); err != nil {
		r.s.l.Errorw("Invalid request query:", "error", err)
		e(ctx, http.StatusBadRequest, fmt.Errorf("invalid request query"))
		return
	}

	if _, ok := validDays[reqQueries.Days]; !ok {
		r.s.l.Errorw("Invalid request query:", "error", reqQueries.Days)
		e(ctx, http.StatusBadRequest, fmt.Errorf("invalid request query"))
		return
	}

	vsCurrency := strings.ToLower(reqQueries.Currency) // Optional query param, default usd.
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
		r.s.l.Errorw("Invalid request query: fiat name not whitelisted", "fiat name:", vsCurrency)
		e(ctx, http.StatusBadRequest, fmt.Errorf("invalid request query: fiat name not whitelisted"))
		return
	}

	coinId := ctx.Param("id")
	if coinId == "" {
		r.s.l.Errorw("Invalid request id: coinID empty")
		e(ctx, http.StatusBadRequest, fmt.Errorf("invalid request id: coinID empty"))
		return
	}
	geckoClient := gecko.NewClient(&http.Client{Timeout: r.s.c.HttpClientTimeout})
	chartData, err := r.s.sh.GetChartData(ctx.Request.Context(), coinId, reqQueries.Days, reqQueries.Currency, geckoClient)
	if err != nil {
		r.s.l.Errorw("Store.GetChartData()", "error", err)
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
