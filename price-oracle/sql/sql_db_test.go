package sql

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach-go/v2/testserver"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	testServer := setup(t)
	defer tearDown(testServer)

	connStr := testServer.PGURL().String()
	require.NotNil(t, connStr)

	mDB, err := NewDB(connStr)
	require.NoError(t, err)
	require.Equal(t, mDB.GetConnectionString(), connStr)
	defer mDB.Close()

	err = mDB.Init()
	require.NoError(t, err)

	rows, err := mDB.Query("SHOW TABLES FROM oracle")
	require.NoError(t, err)
	require.NotNil(t, rows)

	var tableCountDB int
	for rows.Next() {
		tableCountDB++
	}
	err = rows.Err()
	require.NoError(t, err)

	err = rows.Close()
	require.NoError(t, err)

	var tableCountMigration int
	for _, migrationQuery := range migrationList {
		if strings.HasPrefix(strings.TrimPrefix(migrationQuery, "\n"), "CREATE TABLE") {
			tableCountMigration++
		}
	}

	rows, _ = mDB.Query("SELECT * FROM oracle.coingecko")
	require.NotNil(t, rows)

	for rows.Next() {
		tableCountDB++
	}
	err = rows.Err()
	require.NoError(t, err)

	err = rows.Close()
	require.NoError(t, err)

	for _, migrationQueryCoingecko := range migrationCoingecko {
		if strings.HasPrefix(strings.TrimPrefix(migrationQueryCoingecko, "\n"), "CREATE TABLE") {
			tableCountMigration++
		}
	}

	require.Equal(t, tableCountMigration, tableCountDB)
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
