package rest

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"net/http"
	"testing"
)

func TestGeckoIdHandler(t *testing.T) {
	//t.SkipNow()
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

	resp, err := http.Get(fmt.Sprintf("http://%s/geckoid?names=atom,btc", router.s.c.ListenAddr))
	require.NoError(t, err)

	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)

	err = resp.Body.Close()
	require.NoError(t, err)

	var got struct {
		Data map[string]string `json:"data"`
	}
	err = json.Unmarshal(body, &got)
	require.NoError(t, err)

	require.NotNil(t, got.Data)
	require.Equal(t, map[string]string{"atom": "cosmos", "btc": ""}, got.Data)
}

func TestGeckoIdHandler_noParameter(t *testing.T) {
	//t.SkipNow()
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

	resp, err := http.Get(fmt.Sprintf("http://%s/geckoid", router.s.c.ListenAddr))
	require.NoError(t, err)

	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)

	err = resp.Body.Close()
	require.NoError(t, err)

	var got struct {
		Data map[string]string `json:"data"`
	}
	err = json.Unmarshal(body, &got)
	require.NoError(t, err)

	require.NotNil(t, got.Data)
	require.Equal(t, map[string]string{"atom": "cosmos", "luna": "terra-luna"}, got.Data)
}
