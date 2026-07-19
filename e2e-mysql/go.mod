module github.com/moznion/go-txnproof/e2e-mysql

go 1.25.0
toolchain go1.26.5

replace github.com/moznion/go-txnproof => ../

require (
	github.com/go-sql-driver/mysql v1.9.3
	github.com/moznion/go-txnproof v0.0.0-00010101000000-000000000000
)

require filippo.io/edwards25519 v1.1.0 // indirect
