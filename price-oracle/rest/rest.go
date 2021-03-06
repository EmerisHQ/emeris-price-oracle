package rest

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/getsentry/sentry-go"

	"github.com/emerishq/emeris-price-oracle/price-oracle/store"
	ginzap "github.com/gin-contrib/zap"

	"github.com/go-playground/validator/v10"

	"github.com/emerishq/emeris-price-oracle/price-oracle/config"
	"github.com/emerishq/emeris-utils/logging"
	"github.com/emerishq/emeris-utils/sentryx"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type Server struct {
	l  *zap.SugaredLogger
	sh *store.Handler
	g  *gin.Engine
	c  *config.Config
}

type router struct {
	s *Server
}

func NewServer(sh *store.Handler, l *zap.SugaredLogger, c *config.Config) *Server {
	if !c.Debug {
		gin.SetMode(gin.ReleaseMode)
	}

	g := gin.New()

	s := &Server{
		l:  l,
		g:  g,
		sh: sh,
		c:  c,
	}

	r := &router{s: s}

	errAssetLimitExceed = errors.New("more than " + strconv.Itoa(sh.Cfg.MaxAssetsReq) + " asset not allowed")

	g.Use(logging.LogRequest(l.Desugar()))

	g.Use(ginzap.RecoveryWithZap(l.Desugar(), true))
	g.Use(sentryx.GinMiddleware)

	g.GET(r.getAllPrices())
	g.GET(r.getChartData())
	g.GET(r.getGeckoId())
	g.POST(r.getTokensPriceAndSupplies())
	g.POST(r.getFiatsPrices())

	g.NoRoute(func(context *gin.Context) {
		e(context, http.StatusNotFound, errors.New("not found"))
	})

	return s
}

func (s *Server) Serve(where string) error {
	return s.g.Run(where)
}

type restError struct {
	Error string `json:"error"`
}

type restValidationError struct {
	ValidationErrors []string `json:"validation_errors"`
}

var (
	errAssetLimitExceed    = errors.New("more than 10 asset not allowed")
	errZeroAsset           = errors.New("0 asset not allowed")
	errNilAsset            = errors.New("nil asset not allowed")
	errNotWhitelistedAsset = errors.New("not whitelisted asset")
)

// e writes err to the caller, with the given HTTP status.
func e(c *gin.Context, status int, err error) {
	hub := sentry.GetHubFromContext(c.Request.Context())
	hub.WithScope(func(scope *sentry.Scope) {
		scope.SetTag("error", "rest")
		hub.CaptureException(err)
	})
	var jsonErr interface{}
	jsonErr = restError{
		Error: err.Error(),
	}

	ve := validator.ValidationErrors{}
	if errors.As(err, &ve) {
		rve := restValidationError{}
		for _, v := range ve {
			rve.ValidationErrors = append(rve.ValidationErrors, v.Error())
		}

		jsonErr = rve
	}

	_ = c.Error(err)
	c.AbortWithStatusJSON(status, jsonErr)
}

// isSubset returns true if all element of (param:<subList>) in found in (param:<globalList>)
func isSubset(subList []string, globalList []string) bool {
	// Turn globalList into a map
	globalSet := make(map[string]bool, len(globalList))
	for _, s := range globalList {
		globalSet[s] = true
	}

	for _, s := range subList {
		if _, ok := globalSet[s]; !ok {
			return false
		}
	}
	return true
}
