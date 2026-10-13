// Package postgres is the PostgreSQL engine for go-messaging's outbox and
// inbox. [Outbox] returns the statements outbox.New runs, [Inbox] the
// statement inbox.New runs, and [Migrations] the tables they run against.
//
// The migration set is named [Source] and records its history in [Table].
// It holds both the outbox's table and the inbox's, so one module ships them
// in one migration. Every object it creates is prefixed messaging_, and a
// consumer declares the set ahead of its own. An outbox row the relay
// quarantines stays in messaging_outbox with the time and reason of its
// quarantine, and is neither claimed again nor counted by [Pending].
//
// The statements are authored files compiled through sqlate's query
// package; every one that writes requires a transaction. [Verify] prepares
// them against the live schema, for a consumer's verify stage, and
// [Statements] lists them.
//
// The pgtest package gives each test a throwaway database, for this
// module's integration suite and a consumer's own.
package postgres
