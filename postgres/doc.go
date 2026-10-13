// Package postgres is the PostgreSQL engine for go-messaging's outbox and
// inbox.
//
// The package exports:
//
//   - [Outbox], the statements outbox.New runs, and [Inbox], the statement
//     inbox.New runs
//   - [Migrations], the outbox's and the inbox's tables as a migration set,
//     named [Source] and recorded in the history table [Table]
//   - [Verify], which prepares the statements against a live schema, and
//     [Statements], which lists them
//   - [Pending], which counts the outbox rows awaiting publication
//
// # Schema
//
// The migration set holds both the outbox's table and the inbox's, so one
// module ships them in one migration. Every object it creates is prefixed
// messaging_, and a consumer declares the set ahead of its own. An outbox
// row the relay quarantines stays in messaging_outbox with the time and
// reason of its quarantine, quarantined_at and quarantine_reason, and is
// neither claimed again nor counted by [Pending]. A released migration
// never changes; a golden-hash test pins each one.
//
// # Statements
//
// The statements are authored files compiled through sqlate's query
// package; every one that writes requires a transaction. [Verify] is for a
// consumer's verify stage, after its migrations.
//
// The pgtest package gives each test a throwaway database, for this
// module's integration suite and a consumer's own.
package postgres
