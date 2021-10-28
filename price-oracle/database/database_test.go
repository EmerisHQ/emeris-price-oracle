package database_test

import (
	"strings"
	"testing"

	"github.com/allinbits/emeris-price-oracle/price-oracle/database"
	"github.com/cockroachdb/cockroach-go/v2/testserver"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	testServer := setup(t)
	defer tearDown(testServer)

	connStr := testServer.PGURL().String()
	require.NotNil(t, connStr)

	instance, err := database.New(connStr)
	require.NoError(t, err)
	require.Equal(t, instance.GetConnectionString(), connStr)

	rows, err := instance.Query("SHOW TABLES FROM oracle")
	require.NotNil(t, rows)
	require.NoError(t, err)

	var tableCountDB int
	for rows.Next() {
		tableCountDB++
	}
	err = rows.Err()
	require.NoError(t, err)

	err = rows.Close()
	require.NoError(t, err)

	var tableCountMigration int
	for _, migrationQuery := range database.MigrationList {
		if strings.HasPrefix(strings.TrimPrefix(migrationQuery, "\n"), "CREATE TABLE") {
			tableCountMigration++
		}
	}

	rows, err = instance.Query("SELECT * FROM oracle.coingecko")
	require.NotNil(t, rows)
	require.NoError(t, err)

	for rows.Next() {
		tableCountDB++
	}
	err = rows.Err()
	require.NoError(t, err)

	err = rows.Close()
	require.NoError(t, err)

	for _, migrationQueryCoingecko := range database.MigrationListCoingecko {
		if strings.HasPrefix(strings.TrimPrefix(migrationQueryCoingecko, "\n"), "CREATE TABLE") {
			tableCountMigration++
		}
	}

	require.Equal(t, tableCountMigration, tableCountDB)
}

func TestCnstokenQueryHandler(t *testing.T) {
	testServer := setup(t)
	defer tearDown(testServer)

	instance, err := database.New(testServer.PGURL().String())
	require.NoError(t, err)

	_, err = instance.CnstokenQueryHandler()
	require.Error(t, err)
}

func TestCnsPriceIdQueryHandler(t *testing.T) {
	testServer := setup(t)
	defer tearDown(testServer)

	instance, err := database.New(testServer.PGURL().String())
	require.NoError(t, err)

	_, err = instance.CnsPriceIdQueryHandler()
	require.Error(t, err)
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
