# price-oracle

![example workflow](https://github.com/allinbits/emeris-price-oracle/actions/workflows/ci.yml/badge.svg)
[![Build docker images](https://github.com/allinbits/emeris-price-oracle/actions/workflows/docker-build.yml/badge.svg)](https://github.com/allinbits/emeris-price-oracle/actions/workflows/docker-build.yml)
[![codecov](https://codecov.io/gh/allinbits/emeris-price-oracle/branch/main/graph/badge.svg?token=MNF1SLOK6P)](https://codecov.io/gh/allinbits/emeris-price-oracle)
[![Build status](https://github.com/allinbits/emeris-price-oracle/workflows/Build/badge.svg)](https://github.com/allinbits/emeris-price-oracle/commits/main)
[![Tests status](https://github.com/allinbits/emeris-price-oracle/workflows/Tests/badge.svg)](https://github.com/allinbits/emeris-price-oracle/commits/main)
[![Lint](https://github.com/allinbits/emeris-price-oracle/workflows/Lint/badge.svg?token)](https://github.com/allinbits/emeris-price-oracle/commits/main)

Aggregates and caches price data feeds from external data providers.

## Usage

### Configuration
Description of the `emeris-price-oracle.toml` setting file.

Key Feature Settings

- interval: The delay time of the function for an external price provider api request.
- whitelistfiats : List of fiats you want to request (default currency is USD)
- coinmarketcapapikey : This is the api-key of the provider.
- fixerapikey : This is the api-key of the provider.
- Provider : The endpoint address of the price provider.

For Binance, apikey does not exist.

example(Local exec)
```bash
#kubectl port-forward service/cockroachdb-public 26257
DatabaseConnectionURL = "postgres://root@127.0.0.1:26257?sslmode=disable"
ListenAddr = "127.0.0.1:9898"
Debug = true
LogPath = "/home/ubuntu/log"
interval = "10s"
whitelistfiats = ["EUR","KRW","CHF"]
#Not currently used, but may be used in the future
coinmarketcapapikey =""
fixerapikey = ""
```

### Local exec DB
`database/schema`
Set the cockroach DB to the local cluster, connect to the local DB, and run schema as it is.

*The cns version brunch does not require a separate run.

### Build

```bash
# build
go build

# executable
./navigator-price-oracle
```

### Use
Given an exchange API, the price oracle will periodically check with it prices of the tokens we're interested in monitoring, and cache the result until the next period:

1. at startup, oracle will grab data from the API and expose it on `GET /prices`
2. once every 10 seconds, new data will be downloaded and cached

Oracle must return prices of all the tokens that it is configured to fetch.

An API to provide tokens configuration must be provided on `POST /tokens`:

```jsx
{
	"tokens": [
		"ATOMUSDT",
		"KAVAUSDT"
	]
}
```

The same must be done for FIAT currencies on `POST /fiats`:

```jsx
{
	"fiats": [
		"USDEUR",
		"USDKRW"
	]
}
```

### Possible Problems
If you have multiple copy of in-memory cockroach db, it can happen that when you run

`testserver.NewTestServer()` it can stuck at `waiting for download of SOME TMP DIR`

**Solution:** if this happens manually delete **ALL** instances of test server from your machine.

### Known issue
When try to download some private module from `allinbits` you can get:
`fatal: could not read Username for 'https://github.com': terminal prompts disabled`

**Solution:** Set the goprivate env variable. 

go env -w GOPRIVATE="github.com/allinbits/{module}"

## Dependencies & Licenses

The list of non-{Cosmos, AiB, Tendermint} dependencies and their licenses are:

|Module   	                  |License          |
|---	                      |---  	        |
|alicebob/miniredis           |MIT    	        |
|gin-contrib/zap   	          |MIT    	        |
|gin-gonic/gin   	          |MIT   	        |
|go-playground/validator   	  |MIT   	        |
|go-playground/validator   	  |MIT   	        |
|go.uber.org/zap   	          |MIT           	|
|jackc/pgx         	          |MIT    	        |
|jmoiron/sqlx   	          |MIT   	        |
|lib/pq                       |Open use         |
|sigs.k8s.io/controller-runtime |MIT            |
|sony/sonyflake               |MIT              |
|superoo7/go-gecko            |MIT              |

