package config

import (
	"time"

	"github.com/emerishq/emeris-utils/configuration"
	"github.com/emerishq/emeris-utils/validation"
	"github.com/go-playground/validator/v10"
)

type Config struct {
	Debug                 bool
	LogPath               string
	DatabaseConnectionURL string        `validate:"required"`
	ListenAddr            string        `validate:"required"`
	Interval              string        `validate:"required"`
	WhitelistedFiats      []string      `validate:"required"`
	MaxAssetsReq          int           `validate:"required"`
	FixerApiKey           string        `validate:"required"`
	RecoverCount          int           `validate:"required"`
	WorkerPulse           time.Duration `validate:"required"`
	HttpClientTimeout     time.Duration `validate:"required"`

	SentryDSN              string
	SentryEnvironment      string
	SentrySampleRate       float64
	SentryTracesSampleRate float64

	// Not currently used, but may be used in the future
	// CoinmarketcapapiKey string `validate:"required"`
}

func (c Config) Validate() error {
	err := validator.New().Struct(c)
	if err != nil {
		return validation.MissingFieldsErr(err, false)
	}
	_, err = time.ParseDuration(c.Interval)
	if err != nil {
		return err
	}

	return nil
}

func Read() (*Config, error) {
	var c Config

	return &c, configuration.ReadConfig(&c, "emeris-price-oracle", map[string]string{
		"SentryEnvironment":      "notset",
		"SentrySampleRate":       "1.0",
		"SentryTracesSampleRate": "0.3",
	})
}
