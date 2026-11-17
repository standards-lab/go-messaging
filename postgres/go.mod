module github.com/standards-lab/go-messaging/postgres

go 1.27

require (
	github.com/standards-lab/go-core v0.12.0
	github.com/standards-lab/go-messaging v0.3.0
	github.com/standards-lab/sqlate v0.7.0
	github.com/standards-lab/sqlate/postgres v0.8.0
)

require (
	github.com/BurntSushi/toml v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.13.0 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	github.com/standards-lab/sqlate/sqlint v0.4.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/text v0.41.0 // indirect
)

tool github.com/standards-lab/sqlate/sqlint/cmd/sqlint

// Temporary: sqlate/postgres v0.8.0, pgtest's new home, is not yet tagged,
// so the integration suite reaches it in the sibling sqlate checkout. When
// v0.8.0 is tagged, SHIP removes this replace and the requirement above
// resolves from the proxy.
replace github.com/standards-lab/sqlate/postgres => ../../sqlate/postgres
