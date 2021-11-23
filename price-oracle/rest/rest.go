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
	g.POST(r.getselectTokensPrices())
	g.POST(r.getselectFiatsPrices())

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

// SamePair returns true is both lists are same else false
func SamePair(a []string, b []string) bool {
	// Turn b into a map
	m := make(map[string]bool, len(b))
	for _, s := range b {
		m[s] = false
	}
	// Append values from the longest slice that don't exist in the map
	for _, s := range a {
		if _, ok := m[s]; !ok {
			return false
		}
		m[s] = true
	}
	return true
}
