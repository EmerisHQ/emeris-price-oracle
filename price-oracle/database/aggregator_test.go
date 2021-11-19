package database_test

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/allinbits/emeris-price-oracle/price-oracle/daemon"

	models "github.com/allinbits/demeris-backend-models/cns"
	cnsDB "github.com/allinbits/emeris-cns-server/cns/database"
	"github.com/allinbits/emeris-price-oracle/price-oracle/config"
	"github.com/allinbits/emeris-price-oracle/price-oracle/database"
	dbutils "github.com/allinbits/emeris-price-oracle/utils/database"
	"github.com/allinbits/emeris-price-oracle/utils/logging"
	"github.com/cockroachdb/cockroach-go/v2/testserver"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestMain(m *testing.M) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	zap.ReplaceGlobals(logger)
	os.Exit(m.Run())
}

func TestStartAggregate(t *testing.T) {
	ctx, cancel, logger, cfg, tDown := setupAgg(t)
	defer tDown()
	defer cancel()

	atomPrice, lunaPrice := getAggTokenPrice(t, cfg.DatabaseConnectionURL)
	require.Equal(t, atomPrice, 10.0)
	require.Equal(t, lunaPrice, 10.0)

	go database.StartAggregate(ctx, logger, cfg, 3)

	// Validate data updated on DB ..
	require.Eventually(t, func() bool {
		atomPrice, lunaPrice = getAggTokenPrice(t, cfg.DatabaseConnectionURL)
		return atomPrice == 15.0 && lunaPrice == 16.0

	}, 25*time.Second, 2*time.Second)
}

func TestAggregateManager_closes(t *testing.T) {
	_, cancel, logger, cfg, tDown := setupAgg(t)
	defer tDown()
	defer cancel()

	instance, err := database.New(cfg.DatabaseConnectionURL)
	require.NoError(t, err)

	runAsDaemon := daemon.MakeDaemon(10*time.Second, 2, database.AggregateManager)
	done := make(chan struct{})
	hbCh, errCh := runAsDaemon(done, 100*time.Millisecond, instance.GetDB(), logger, cfg, database.PricefiatAggregator)

	// Collect 5 heartbeats and then close
	for i := 0; i < 5; i++ {
		<-hbCh
	}
	close(done)
	_, ok := <-hbCh
	require.Equal(t, false, ok)
	_, ok = <-errCh
	require.Equal(t, false, ok)
}

func TestAggregateManager_worker_restarts(t *testing.T) {
	_, cancel, logger, cfg, tDown := setupAgg(t)
	defer tDown()
	defer cancel()

	instance, err := database.New(cfg.DatabaseConnectionURL)
	require.NoError(t, err)

	numRecover := 2
	runAsDaemon := daemon.MakeDaemon(10*time.Second, numRecover, database.AggregateManager)
	done := make(chan struct{})
	db := instance.GetDB()
	hbCh, errCh := runAsDaemon(done, 6*time.Second, db, logger, cfg, database.PricefiatAggregator)

	// Wait for the process to start
	<-hbCh
	// Close the DB
	err = db.Close()
	require.NoError(t, err)
	// Collect 2 error logs
	for i := 0; i < numRecover; i++ {
		require.Equal(t, "sql: database is closed", (<-errCh).Error())
	}
	// Ensure everything is closed
	_, ok := <-errCh
	require.Equal(t, false, ok)
	close(done)
	_, ok = <-hbCh
	require.Equal(t, false, ok)
}

func setupAgg(t *testing.T) (context.Context, func(), *zap.SugaredLogger, *config.Config, func()) {
	t.Helper()
	testServer, err := testserver.NewTestServer()
	require.NoError(t, err)
	require.NoError(t, testServer.WaitForInit())

	connStr := testServer.PGURL().String()
	require.NotNil(t, connStr)

	// Seed DB with data in schema file
	oracleMigration := readLinesFromFile(t, "schema-unittest")
	err = dbutils.RunMigrations(connStr, oracleMigration)
	require.NoError(t, err)

	cfg := &config.Config{ // config.Read() is not working. Fixing is not in scope of this task. That comes later.
		LogPath:               "",
		Debug:                 true,
		DatabaseConnectionURL: connStr,
		Interval:              "10s",
		Whitelistfiats:        []string{"EUR", "KRW", "CHF"},
	}

	logger := logging.New(logging.LoggingConfig{
		LogPath: cfg.LogPath,
		Debug:   cfg.Debug,
	})

	insertToken(t, connStr)
	ctx, cancel := context.WithCancel(context.Background())
	return ctx, cancel, logger, cfg, func() { testServer.Stop() }
}

func insertToken(t *testing.T, connStr string) {
	chain := models.Chain{
		ChainName:        "cosmos-hub",
		DemerisAddresses: []string{"addr1"},
		DisplayName:      "ATOM display name",
		GenesisHash:      "hash",
		NodeInfo:         models.NodeInfo{},
		ValidBlockThresh: models.Threshold(1 * time.Second),
		DerivationPath:   "derivation_path",
		SupportedWallets: []string{"metamask"},
		Logo:             "logo 1",
		Denoms: []models.Denom{
			{
				Name:        "ATOM",
				PriceID:     "cosmos",
				DisplayName: "ATOM",
				FetchPrice:  true,
				Ticker:      "ATOM",
			},
			{
				Name:        "LUNA",
				PriceID:     "terra-luna",
				DisplayName: "LUNA",
				FetchPrice:  true,
				Ticker:      "LUNA",
			},
		},
		PrimaryChannel: models.DbStringMap{
			"cosmos-hub":  "ch0",
			"persistence": "ch2",
		},
	}
	cnsInstanceDB, err := cnsDB.New(connStr)
	require.NoError(t, err)

	err = cnsInstanceDB.AddChain(chain)
	require.NoError(t, err)

	cc, err := cnsInstanceDB.Chains()
	require.NoError(t, err)
	require.Equal(t, 1, len(cc))
}

func getAggTokenPrice(t *testing.T, connStr string) (float64, float64) {
	instance, err := database.New(connStr)
	require.NoError(t, err)

	tokenPrice := make(map[string]float64)
	rows, err := instance.Query("SELECT * FROM oracle.tokens")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var tokenName string
		var price float64
		err := rows.Scan(&tokenName, &price)
		require.NoError(t, err)
		tokenPrice[tokenName] = price
	}
	return tokenPrice["ATOMUSDT"], tokenPrice["LUNAUSDT"]
}

func readLinesFromFile(t *testing.T, s string) []string {
	file, err := os.Open(s)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	var commands []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		cmd := scanner.Text()
		commands = append(commands, cmd)
	}
	return commands
}

func TestAveraging(t *testing.T) {
	nums := map[string]float64{
		"a": 1.1,
		"b": 2.2,
		"c": 3.3,
		"d": 4.4,
		"e": 5.5,
	}
	avg, err := database.Averaging(nums)
	require.NoError(t, err)
	require.Equal(t, 3.3, avg)

	_, err = database.Averaging(nil)
	require.Error(t, err)
	require.Equal(t, fmt.Errorf("nil price list recieved"), err)

	_, err = database.Averaging(map[string]float64{})
	require.Error(t, err)
	require.Equal(t, fmt.Errorf("empty price list recieved"), err)
}
