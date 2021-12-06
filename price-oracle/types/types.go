package types

import (
	"encoding/json"
	"time"
)

const (
	USDT = "USDT"
	USD  = "USD"
)

type AllPriceResponse struct {
	Tokens []TokenPriceAndSupply
	Fiats  []FiatPrice
}

type TokenPriceAndSupply struct {
	Symbol string  `db:"symbol"`
	Price  float64 `db:"price"`
	Supply float64 `db:"supply"`
}
type FiatPrice struct {
	Symbol string  `db:"symbol"`
	Price  float64 `db:"price"`
}

type Prices struct {
	Symbol    string  `db:"symbol"`
	Price     float64 `db:"price"`
	UpdatedAt int64   `db:"updatedat"`
}

type Tokens struct {
	Tokens []string `json:"tokens"`
}

type Fiats struct {
	Fiats []string `json:"fiats"`
}

type Binance struct {
	Symbol string `json:"symbol"`
	Price  string `json:"price"`
}

type Coinmarketcap struct {
	Status struct {
		Timestamp    time.Time   `json:"timestamp"`
		ErrorCode    int         `json:"error_code"`
		ErrorMessage interface{} `json:"error_message"`
		Elapsed      int         `json:"elapsed"`
		CreditCount  int         `json:"credit_count"`
		Notice       interface{} `json:"notice"`
	} `json:"status"`
	Data json.RawMessage `json:"data"`
}

type Fixer struct {
	Success bool            `json:"success"`
	Rates   json.RawMessage `json:"rates"`
}
