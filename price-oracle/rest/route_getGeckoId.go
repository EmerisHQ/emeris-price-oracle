package rest

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

const getGeckoId = "/geckoid"

func (r *router) geckoIdHandler(ctx *gin.Context) {
	namesAsString := ctx.Query("names")
	var names []string
	if namesAsString != "" {
		names = strings.Split(namesAsString, ",")
	}

	geckoNameToId, err := r.s.sh.GetGeckoIdForTokenNames(ctx, names)
	if err != nil {
		r.s.l.Errorw("Store.GetGeckoIdForTokenNames()", "Error", err)
		e(ctx, http.StatusInternalServerError, err)
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"status":  http.StatusOK,
		"data":    geckoNameToId,
		"message": nil,
	})
}

func (r *router) getGeckoId() (string, gin.HandlerFunc) {
	return getGeckoId, r.geckoIdHandler
}
