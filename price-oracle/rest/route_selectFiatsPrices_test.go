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

func TestSelectFiatsPrice(t *testing.T) {
	router, ctx, w, tDown := setup(t)
	defer tDown()

	want := []types.FiatPrice{
		{Symbol: "USDEUR", Price: 20},
		{Symbol: "USDKRW", Price: 5},
	}

	err := insertWantData(router, types.AllPriceResponse{Fiats: want})
	require.NoError(t, err)

	ctx.Request = &http.Request{
		Header: make(http.Header),
	}
	ctx.Request.Method = "POST" // or PUT
	ctx.Request.Header.Set("Content-Type", "application/json")

	fiats := types.Fiats{
		Fiats: []string{"USDEUR", "USDKRW"},
	}
	jsonBytes, err := json.Marshal(fiats)
	require.NoError(t, err)
	ctx.Request.Body = io.NopCloser(bytes.NewBuffer(jsonBytes))

	_, handler := router.getFiatsPrices()
	handler(ctx)

	var got struct {
		Data []types.FiatPrice `json:"data"`
	}
	err = json.Unmarshal(w.Body.Bytes(), &got)
	require.NoError(t, err)

	require.Equal(t, want, got.Data)
}
