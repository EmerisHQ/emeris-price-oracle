package database

import "github.com/allinbits/emeris-utils/database"

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

const createTableTokens = `CREATE TABLE oracle.tokens (symbol STRING PRIMARY KEY, price FLOAT);`

const createTableFiats = `CREATE TABLE oracle.fiats (symbol STRING PRIMARY KEY, price FLOAT);`

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

func (i *Instance) runMigrations() {
	if err := database.RunMigrations(i.connString, migrationList); err != nil {
		panic(err)
	}
}

func (i *Instance) runMigrationsCoingecko() {
	if err := database.RunMigrations(i.connString, migrationCoingecko); err != nil {
		panic(err)
	}
}
