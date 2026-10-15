package postgres

import (
	"embed"
	"fmt"

	"github.com/standards-lab/sqlate/migrate"
)

const (
	// Source is the migration set's name and the prefix of every object the
	// set owns.
	Source = "messaging"

	// Table is the history table the set's migrations are recorded in, apart
	// from a consumer's own.
	Table = "messaging_schema_version"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// Migrations returns the messaging migration set for Postgres: the outbox's
// and the inbox's tables, and the indexes their retentions read. A consumer
// declares it ahead of its own set in its migrator.
func Migrations() (migrate.Set, error) {
	files, err := migrate.Files(migrationFiles, "migrations")
	if err != nil {
		return migrate.Set{}, fmt.Errorf("go-messaging/postgres: %w", err)
	}
	return migrate.Set{Name: Source, Table: Table, Migrations: files}, nil
}
