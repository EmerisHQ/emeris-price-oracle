package config

import (
	"time"

	"github.com/allinbits/emeris-price-oracle/utils/configuration"
	"github.com/allinbits/emeris-price-oracle/utils/validation"
	"github.com/go-playground/validator/v10"
)

type Config struct {
	DatabaseConnectionURL string `validate:"required"`
	ListenAddr            string `validate:"required"`
	Debug                 bool
	LogPath               string
	HttpClientTimeOut     time.Duration
	Interval              string `validate:"required"`
	RecoverCount          int
	WorkerPulse           time.Duration `validate:"required"`
	Whitelistfiats        []string      `validate:"required"`
	MaxAssetsReq          int
	//Not currently used, but may be used in the future
	//CoinmarketcapapiKey string `validate:"required"`
	RedisUrl    string        `validate:"required"`
	Fixerapikey string        `validate:"required"`
	RedisExpiry time.Duration `validate:"required"`
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

	return &c, configuration.ReadConfig(&c, "demeris-price-oracle", map[string]string{})
}
