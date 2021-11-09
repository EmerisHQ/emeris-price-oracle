package daemon

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOr(t *testing.T) {
	ch1 := make(chan struct{})
	orCh := or(ch1)
	close(ch1)
	_, ok := <-orCh
	require.Equal(t, false, ok)

	ch1 = make(chan struct{})
	ch2 := make(chan struct{})
	orCh = or(ch1, ch2)
	close(ch1)
	_, ok = <-orCh
	require.Equal(t, false, ok)

	ch1 = make(chan struct{})
	ch2 = make(chan struct{})
	ch3 := make(chan struct{})
	orCh = or(ch1, ch2, ch3)
	close(ch1)
	_, ok = <-orCh
	require.Equal(t, false, ok)

	ch1 = make(chan struct{})
	ch2 = make(chan struct{})
	ch3 = make(chan struct{})
	ch4 := make(chan struct{})
	orCh = or(ch1, ch2, ch3, ch4)
	close(ch4)
	_, ok = <-orCh
	require.Equal(t, false, ok)
}
