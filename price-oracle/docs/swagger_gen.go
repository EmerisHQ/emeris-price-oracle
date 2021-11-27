//go:generate go run github.com/swaggo/swag/cmd/swag i -g ../docs/swagger_gen.go -d ../rest --parseDepth 3 --parseDependency -o ./

// @title Emeris Price Oracle API
// @version 1.0
// @description External API aggregating 3rd party price oracle data.

// @contact.name API Support
// @contact.email gautier@tendermint.com

// @BasePath /
// @query.collection.format multi

// Package docs is needed to generate swagger documentation.
// We keep underscore import here to make sure go mod doesn't remove swaggo dependency,
// otherwise we cannot use the generate statement up there ^.
package docs

import (
	_ "github.com/allinbits/emeris-price-oracle/utils/store"
	_ "github.com/cosmos/cosmos-sdk/types"
	_ "github.com/cosmos/cosmos-sdk/types/tx"
	_ "github.com/cosmos/cosmos-sdk/x/bank/types"
	_ "github.com/swaggo/swag"
	_ "github.com/tendermint/tendermint/proto/tendermint/version"
	_ "github.com/tendermint/tendermint/rpc/core/types"
)
