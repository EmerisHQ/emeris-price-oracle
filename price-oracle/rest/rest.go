package rest

import (
	"errors"
	"github.com/allinbits/emeris-price-oracle/price-oracle/store"
	"net/http"

	"github.com/go-playground/validator/v10"

	"github.com/allinbits/emeris-price-oracle/price-oracle/config"
	"github.com/allinbits/emeris-price-oracle/utils/logging"
	redis "github.com/allinbits/emeris-price-oracle/utils/store"
	ginzap "github.com/gin-contrib/zap"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type Server struct {
	l  *zap.SugaredLogger
	sh *store.Handler
	g  *gin.Engine
	c  *config.Config
	ri *redis.Store
}

type router struct {
	s *Server
}

func NewServer(sh *store.Handler, ri *redis.Store, l *zap.SugaredLogger, c *config.Config) *Server {
	if !c.Debug {
		gin.SetMode(gin.ReleaseMode)
	}

	g := gin.New()

	s := &Server{
		l:  l,
		g:  g,
		sh: sh,
		c:  c,
		ri: ri,
	}

	r := &router{s: s}

	g.Use(logging.LogRequest(l.Desugar()))
	g.Use(ginzap.RecoveryWithZap(l.Desugar(), true))

	g.GET(r.getAllPrices())
	g.POST(r.getTokensPrices())
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
	errAssetLimitExceed    = errors.New("not allow More than 10 asset")
	errZeroAsset           = errors.New("not allow 0 asset")
	errNilAsset            = errors.New("not allow nil asset")
	errNotWhitelistedAsset = errors.New("not whitelisted asset")
)

// e writes err to the caller, with the given HTTP status.
func e(c *gin.Context, status int, err error) {
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

// IsSubset returns true if all element of subList in found in globalList
func IsSubset(subList []string, globalList []string) bool {
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
