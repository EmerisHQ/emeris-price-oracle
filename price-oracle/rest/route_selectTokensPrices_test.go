package rest

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/emerishq/emeris-price-oracle/price-oracle/types"
	"github.com/stretchr/testify/require"
)

func TestSelectTokensPrices(t *testing.T) {
	router, ctx, w, tDown := setup(t)
	defer tDown()

	want := []types.TokenPriceAndSupply{
		{Price: 10, Symbol: "ATOMUSDT", Supply: 113563929433.0},
		{Price: 10, Symbol: "LUNAUSDT", Supply: 113563929433.0},
	}
	err := insertWantData(router, types.AllPriceResponse{Tokens: want})
	require.NoError(t, err)

	ctx.Request = &http.Request{
		Header: make(http.Header),
	}
	ctx.Request.Method = "POST" // or PUT
	ctx.Request.Header.Set("Content-Type", "application/json")

	fiats := types.Tokens{
		Tokens: []string{"ATOMUSDT", "LUNAUSDT"},
	}
	jsonBytes, err := json.Marshal(fiats)
	require.NoError(t, err)
	ctx.Request.Body = io.NopCloser(bytes.NewBuffer(jsonBytes))

	_, handler := router.getTokensPriceAndSupplies()
	handler(ctx)

	var got struct {
		Data []types.TokenPriceAndSupply `json:"data"`
	}
	err = json.Unmarshal(w.Body.Bytes(), &got)
	require.NoError(t, err)

	require.Equal(t, want, got.Data)
}
