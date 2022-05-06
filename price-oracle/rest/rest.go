package rest

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/getsentry/sentry-go"

	"github.com/emerishq/emeris-price-oracle/price-oracle/store"

	"github.com/go-playground/validator/v10"

	"github.com/emerishq/emeris-price-oracle/price-oracle/config"
	"github.com/emerishq/emeris-utils/ginsentry"
	"github.com/emerishq/emeris-utils/logging"
	sentrygin "github.com/getsentry/sentry-go/gin"
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

	// TODO: @tbruyelle chk
	g.Use(sentrygin.New(sentrygin.Options{
		Repanic:         true,
		WaitForDelivery: false,
		Timeout:         time.Second * 2,
	}))
	g.Use(span)
	g.Use(catchPanics(l.Desugar()))

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
	// TODO: @tbruyelle chk
	hub := sentrygin.GetHubFromContext(c)
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

// catchPanics returns a gin.HandlerFunc (middleware) that recovers from any
// panics, logs and reports them to sentry.
func catchPanics(logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				ginsentry.Recover(c, err)
				httpRequest, _ := httputil.DumpRequest(c.Request, false)
				logger.Sugar().Errorw("[Recovery from panic]",
					"time", time.Now(),
					"error", err,
					"request", string(httpRequest),
					"stack", string(debug.Stack()),
				)
				c.AbortWithStatus(http.StatusInternalServerError)
			}
		}()
		c.Next()
	}
}

// TODO: @tbruyelle chk
func span(ctx *gin.Context) {
	// start span for the request
	span := sentry.StartSpan(ctx.Request.Context(), "http.server",
		sentry.TransactionName(fmt.Sprintf("%s %s", ctx.Request.Method, ctx.Request.URL.Path)),
		sentry.ContinueFromRequest(ctx.Request),
	)
	defer span.Finish()
	ctx.Request = ctx.Request.WithContext(span.Context())
	ctx.Next()
}
