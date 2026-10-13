module github.com/standards-lab/go-messaging/postgres

go 1.27

require (
	github.com/jackc/pgx/v5 v5.11.0
	github.com/standards-lab/go-core v0.10.0
	github.com/standards-lab/go-messaging v0.1.0
	github.com/standards-lab/sqlate v0.5.0
	github.com/standards-lab/sqlate/postgres v0.5.0
)

require (
	github.com/BurntSushi/toml v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/standards-lab/sqlate/sqlint v0.3.0 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/text v0.29.0 // indirect
)

tool github.com/standards-lab/sqlate/sqlint/cmd/sqlint

// Transient bridge while this module builds against the base module's
// unreleased v0.1.0; the v0.1.0 tag drops it.
replace github.com/standards-lab/go-messaging => ../
