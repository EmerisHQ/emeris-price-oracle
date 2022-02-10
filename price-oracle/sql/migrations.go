package sql

import "fmt"

const createDatabase = `
CREATE DATABASE oracle;
`

const createTableBinance = `
CREATE TABLE IF NOT EXISTS oracle.binance (symbol STRING PRIMARY KEY, price FLOAT, updatedat INT);
`
const createTableCoinmarketcap = `
CREATE TABLE IF NOT EXISTS oracle.coinmarketcap (symbol STRING PRIMARY KEY, price FLOAT, updatedat INT);
`
const createTableCoingecko = `
CREATE TABLE IF NOT EXISTS oracle.coingecko (symbol STRING PRIMARY KEY, price FLOAT, updatedat INT);
`
const createTableCoinmarketcapSupply = `
CREATE TABLE IF NOT EXISTS oracle.coinmarketcapsupply (symbol STRING PRIMARY KEY, supply FLOAT);
`
const createTableCoingeckoSupply = `
CREATE TABLE IF NOT EXISTS oracle.coingeckosupply (symbol STRING PRIMARY KEY, supply FLOAT);
`
const createTableFixer = `
CREATE TABLE IF NOT EXISTS oracle.fixer (symbol STRING PRIMARY KEY, price FLOAT, updatedat INT);
`

const createTableTokens = `CREATE TABLE IF NOT EXISTS oracle.tokens (symbol STRING PRIMARY KEY, price FLOAT);`

const createTableFiats = `CREATE TABLE IF NOT EXISTS oracle.fiats (symbol STRING PRIMARY KEY, price FLOAT);`

var migrationList = []string{
	createTableBinance,
	createTableCoinmarketcap,
	createTableCoinmarketcapSupply,
	createTableFixer,
	createTableTokens,
	createTableFiats,
	createTableCoingecko,
	createTableCoingeckoSupply,
}

func (m *SqlDB) runMigrations() error {
	if err := m.RunMigrations(migrationList); err != nil {
		return err
	}
	return nil
}

func (m *SqlDB) createDatabase() error {
	m, err := NewDB(m.connString)
	if err != nil {
		return err
	}

	if _, err := m.db.Exec(createDatabase); err != nil {
		return fmt.Errorf("error while creating database : %w", err)
	}

	return m.db.Close()
}

func (m *SqlDB) RunMigrations(migrations []string) error {
	m, err := NewDB(m.connString)
	if err != nil {
		return err
	}

	for i, migration := range migrations {

		if _, err := m.db.Exec(migration); err != nil {
			return fmt.Errorf("error while running migration #%d, %w", i, err)
		}
	}

	return m.db.Close()
}
