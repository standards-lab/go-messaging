# Changelog

All notable changes to the PostgreSQL engine (`github.com/standards-lab/go-messaging/postgres`)
are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). This
changelog covers this sub-module only; the base module keeps its own.

## [Unreleased]

## [v0.3.0] - 2026-11-11

### Changed

- **Breaking:** The `go-messaging` requirement is v0.3.0, which builds a service's runtime with
  `messaging.Setup{...}.Build()` in place of `messaging.New` and its `Option`, `OnFailure`, and
  `Gated`, and takes `outbox.Outbox.Relay`'s report and options as one `outbox.RelayOptions`
  in place of its `report` parameter and `RelayOption`, `outbox.Timeout`, `outbox.Drain`, and
  `outbox.Gated`. The base module's v0.3.0 entry migrates each. `Outbox` and `Inbox` are
  unchanged.
- **Breaking:** The `go-core` requirement is v0.12.0, whose `reactor.New` and
  `event.NewRecorder` take a trailing Options struct in place of variadic option functions, and
  which pulls go-core v0.11.0's graph changes into an importer's build. go-core's changelog
  migrates each.
- **Breaking:** The `sqlate` requirement is v0.7.0, whose `DB.Begin`, `DB.Transact`, and
  `Transact` take a trailing `*sql.TxOptions` in place of variadic `TxOption` values, and whose
  `Beginner` has `DB.Begin`'s new signature. sqlate's changelog migrates each. `Verify`,
  `Pending`, and the retentions are unchanged.
- **Breaking:** The `sqlate/postgres` requirement is v0.7.0, which carries the same sqlate
  requirement; the dialect's own API is unchanged.
- pgx is v5.13.0, and the `sqlate/sqlint` tool v0.4.0, whose findings are unchanged.

## [v0.2.0] - 2026-10-15

### Added

- `OutboxRetention` and `InboxRetention`, which return a `*Retention` whose `Pass(ctx, budget)`
  deletes at most `budget` rows older than the retention's age, oldest first, and reports `more`
  exactly when it deleted its whole budget. The outbox's deletes published rows by
  `published_at` and never a row awaiting publication or a quarantined one; the inbox's deletes
  rows by `handled_at`. Both constructors refuse an age that is not positive, a pass refuses a
  budget below 1, and the module exports no default age. Neither is part of an `Engine`.
- Migrations 0002 and 0003 in the `messaging` set: `messaging_ix_outbox_published`, the published
  outbox rows by `published_at`, and `messaging_ix_inbox_handled`, the inbox rows by
  `handled_at`, each built and dropped `CONCURRENTLY` in a non-transactional migration
  (`--| transaction: none`) of one statement. The golden-hash test pins both.
- `Verify` prepares, and `Statements` lists, the retentions' statements, `reclaim_outbox` and
  `reclaim_inbox`.

### Changed

- The module requires `go-messaging` v0.2.0, which adds the `Gated` option and `outbox.Gate`;
  nothing in this module changes with it.

## [v0.1.0] - 2026-10-13

### Added

- `Outbox` and `Inbox`, the outbox's and inbox's statements compiled for PostgreSQL through
  sqlate's query package, for `outbox.New` and `inbox.New`. Every statement that writes requires
  a transaction. The relay's claim locks the oldest row neither published nor quarantined with
  `FOR UPDATE SKIP LOCKED`, so relays share the outbox.
- `Migrations`, the `messaging` migration set named `Source` and recorded in `Table`:
  `messaging_outbox`, with its `quarantined_at` and `quarantine_reason` columns, set together or
  not at all, and `messaging_inbox`. A golden-hash test pins each released migration.
- `Verify`, which prepares the statements against a live schema for a consumer's verify stage;
  `Statements`, which lists them; and `Pending`, which counts the outbox rows awaiting
  publication, quarantined rows excluded.
- `pgtest`, which gives each test a throwaway database: `Scratch`, `Open`, `DSN`, and `Pool`.
- The module requires `go-messaging` v0.1.0, `sqlate` and `sqlate/postgres` v0.5.0, and pgx
  v5.11.0, with the `sqlate/sqlint` tool at v0.3.0.

[Unreleased]: https://github.com/standards-lab/go-messaging/compare/postgres/v0.3.0...HEAD
[v0.3.0]: https://github.com/standards-lab/go-messaging/compare/postgres/v0.2.0...postgres/v0.3.0
[v0.2.0]: https://github.com/standards-lab/go-messaging/compare/postgres/v0.1.0...postgres/v0.2.0
[v0.1.0]: https://github.com/standards-lab/go-messaging/releases/tag/postgres/v0.1.0
