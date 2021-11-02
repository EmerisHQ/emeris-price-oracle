package database

import (
	"testing"

	"github.com/allinbits/emeris-price-oracle/price-oracle/sql"
	"github.com/cockroachdb/cockroach-go/v2/testserver"
	"github.com/stretchr/testify/require"
)

func TestNewDBHandler(t *testing.T) {
	testServer := setup(t)
	defer tearDown(testServer)

	db, err := getdb(t, testServer)
	require.NoError(t, err)

	dbHandler, err := NewDBHandler(db)
	require.NoError(t, err)
	require.NotNil(t, dbHandler.db)
}

func setup(t *testing.T) testserver.TestServer {
	ts, err := testserver.NewTestServer()
	require.NoError(t, err)
	require.NoError(t, ts.WaitForInit())

	return ts
}

func tearDown(ts testserver.TestServer) {
	ts.Stop()
}
func getdb(t *testing.T, ts testserver.TestServer) (*sql.SqlDB, error) {
	connStr := ts.PGURL().String()
	return sql.NewDB(connStr)
}
