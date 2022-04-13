package rest

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"net/http"
	"testing"

	geckoTypes "github.com/superoo7/go-gecko/v3/types"
)

func TestChartData(t *testing.T) {
	router, _, _, tDown := setup(t)
	defer tDown()

	s := NewServer(router.s.sh, router.s.l, router.s.c)
	ch := make(chan struct{})
	go func() {
		close(ch)
		err := s.Serve(router.s.c.ListenAddr)
		if err != nil {
			require.Contains(t, err.Error(), "address already in use")
		}
	}()
	<-ch // Wait for the goroutine to start. Still hack!!

	// Caution: we only have 1 day bitcoin-eur data in cache. If you want to change it to other coins/days/currency
	// test will break.
	resp, err := http.Get(fmt.Sprintf("http://%s/chart/bitcoin?vs_currency=eur&days=1", router.s.c.ListenAddr))
	require.NoError(t, err)

	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)

	err = resp.Body.Close()
	require.NoError(t, err)

	var got struct {
		Data *geckoTypes.CoinsIDMarketChart `json:"data"`
	}

	err = json.Unmarshal(body, &got)
	require.NoError(t, err)

	require.NotNil(t, got.Data.Prices)
	require.Equal(t, got.Data, generateChartData(12*24, 0, 0))
}
