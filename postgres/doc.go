// Package postgres is the PostgreSQL engine for go-messaging's outbox and
// inbox.
//
// The package exports:
//
//   - [Outbox], the statements outbox.New runs, and [Inbox], the statement
//     inbox.New runs
//   - [Migrations], the outbox's and the inbox's tables, and the indexes
//     their retentions read, as a migration set, named [Source] and
//     recorded in the history table [Table]
//   - [Verify], which prepares the statements against a live schema, and
//     [Statements], which lists them
//   - [Pending], which counts the outbox rows awaiting publication
//   - [Retention], a table's retention, built by [OutboxRetention] for the
//     published outbox rows and [InboxRetention] for the handled inbox rows
//
// # Schema
//
// The migration set holds both the outbox's table and the inbox's, so one
// module ships them: 0001 creates both tables, 0002 indexes the published
// outbox rows by published_at, and 0003 the inbox rows by handled_at, for
// the retentions. Every object it creates is prefixed messaging_, and a
// consumer declares the set ahead of its own. An outbox row the relay
// quarantines stays in messaging_outbox with the time and reason of its
// quarantine, quarantined_at and quarantine_reason, and is neither claimed
// again, counted by [Pending], nor deleted by a retention. A released
// migration never changes; a golden-hash test pins each one.
//
// 0002 and 0003 build their indexes CONCURRENTLY, so a table already in
// service keeps taking writes while its index builds. PostgreSQL refuses
// CONCURRENTLY inside a transaction block, so each migration, up and down,
// is headed "--| transaction: none" and holds its one statement; each down
// drops its index CONCURRENTLY. A build that fails leaves the set dirty at
// its version and may leave an invalid index behind: the repair is to drop
// the index, Force the set to the version before, and run Up again.
//
// # Statements
//
// The statements are authored files compiled through sqlate's query
// package. Every one the outbox and the inbox run that writes requires a
// transaction; the count and the retentions' deletes run on a pool.
// [Verify], which prepares them all, the retentions' among them, is for a
// consumer's verify stage, after its migrations.
//
// # Retention
//
// The relay never deletes a published row and the inbox never deletes a
// handled one, so both tables grow until a consumer reclaims them. A
// [Retention] does: each Pass deletes at most its budget of rows older
// than the retention's age, oldest first, in one statement, and reports
// more while rows that were eligible may remain. A pass reports more
// exactly when it deleted its whole budget, so a table holding exactly a
// budget of eligible rows costs one extra, empty pass. Pass has the shape
// of a sweeper's reclaimer, so a sweeper that bounds each pass by a budget
// drives it.
//
// [OutboxRetention] deletes the published rows published more than its age
// ago; it never deletes a row awaiting publication or a quarantined one,
// which awaits an operator. [InboxRetention] deletes the rows handled more
// than its age ago, after which a redelivery of the row's event is claimed
// again, so its age must outlast every redelivery the broker can make. The
// module exports no default age: both constructors refuse an age that is
// not positive, and a pass refuses a budget below 1. The age is measured
// on the calling process's clock. Neither is part of the outbox's or the
// inbox's Engine.
//
// The pgtest package gives each test a throwaway database, for this
// module's integration suite and a consumer's own.
package postgres
