module github.com/allinbits/emeris-price-oracle

go 1.17

replace (
	github.com/gogo/protobuf => github.com/regen-network/protobuf v1.3.3-alpha.regen.1
	github.com/jmoiron/sqlx => github.com/abraithwaite/sqlx v1.3.2-0.20210331022513-df9bf9884350
	k8s.io/client-go => k8s.io/client-go v0.21.1
)

require (
	github.com/allinbits/demeris-backend-models v0.0.0-20211022115747-df6f5881eb00
	github.com/allinbits/emeris-cns-server v0.0.0-20211021152447-99e15c2c0714
	github.com/allinbits/starport-operator v0.0.1-alpha.26
	github.com/cockroachdb/cockroach-go/v2 v2.1.1
	github.com/ethereum/go-ethereum v1.10.3
	github.com/gin-contrib/zap v0.0.1
	github.com/gin-gonic/gin v1.7.1
	github.com/go-playground/universal-translator v0.17.0
	github.com/go-playground/validator/v10 v10.5.0
	github.com/iamolegga/enviper v1.2.1
	github.com/jackc/pgx/v4 v4.10.1
	github.com/jmoiron/sqlx v1.3.3
	github.com/lib/pq v1.10.3
	github.com/natefinch/lumberjack v2.0.0+incompatible
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.7.1-0.20210427113832-6241f9ab9942
	github.com/superoo7/go-gecko v1.0.0
	go.uber.org/zap v1.17.0
	k8s.io/api v0.21.1
	k8s.io/apimachinery v0.21.1
	k8s.io/client-go v11.0.0+incompatible
	sigs.k8s.io/controller-runtime v0.9.0
)
