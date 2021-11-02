package sql

import "fmt"

const createDatabase = `
CREATE DATABASE oracle;
`

const createTableBinance = `
CREATE TABLE oracle.binance (symbol STRING PRIMARY KEY, price FLOAT, updatedat INT);
`
const createTableCoinmarketcap = `
CREATE TABLE oracle.coinmarketcap (symbol STRING PRIMARY KEY, price FLOAT, updatedat INT);
`
const createTableCoingecko = `
CREATE TABLE oracle.coingecko (symbol STRING PRIMARY KEY, price FLOAT, updatedat INT);
`
const createTableCoinmarketcapSupply = `
CREATE TABLE oracle.coinmarketcapsupply (symbol STRING PRIMARY KEY, supply FLOAT);
`
const createTableCoingeckoSupply = `
CREATE TABLE oracle.coingeckosupply (symbol STRING PRIMARY KEY, supply FLOAT);
`
const createTableFixer = `
CREATE TABLE oracle.fixer (symbol STRING PRIMARY KEY, price FLOAT, updatedat INT);
`
const createTableTokens = `
CREATE TABLE oracle.tokens (symbol STRING PRIMARY KEY, price FLOAT);
`
const createTableFiats = `
CREATE TABLE oracle.fiats (symbol STRING PRIMARY KEY, price FLOAT);
`

var migrationList = []string{
	createDatabase,
	createTableBinance,
	createTableCoinmarketcap,
	createTableCoinmarketcapSupply,
	// createTableCoingecko,
	// createTableCoingeckoSupply,
	createTableFixer,
	createTableTokens,
	createTableFiats,
}

var migrationCoingecko = []string{
	createTableCoingecko,
	createTableCoingeckoSupply,
}

func (m *SqlDB) runMigrations() error {
	if err := m.RunMigrations(m.connString, migrationList); err != nil {
		return err
	}
	return nil
}

func (m *SqlDB) runMigrationsCoingecko() error {
	if err := m.RunMigrations(m.connString, migrationCoingecko); err != nil {
		return err
	}
	return nil
}

func (m *SqlDB) RunMigrations(dbConnString string, migrations []string) error {
	m, err := NewDB(dbConnString)
	if err != nil {
		return err
	}

	for i, migration := range migrations {
		_, err := m.db.Exec(migration)
		if err != nil {
			return fmt.Errorf("error while running migration #%d, %w", i, err)
		}
	}

	return m.db.Close()
}
