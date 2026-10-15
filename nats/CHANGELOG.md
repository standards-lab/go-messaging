# Changelog

All notable changes to the NATS JetStream provider (`github.com/standards-lab/go-messaging/nats`)
are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). This
changelog covers this sub-module only; the base module keeps its own.

## [Unreleased]

### Changed

- The module requires `go-messaging` v0.2.0, which adds the `Gated` option and `outbox.Gate`;
  nothing in this module changes with it.

## [v0.2.0] - 2026-10-14

### Changed

- **Breaking:** `Publish` sends an event with its type as the subject, unchanged, rather than
  `Prefix.<type>`, and a subscription's consumer filters on its types as subjects. `Prefix` is now
  the namespace of the broker's event types: the owner's stream still captures `Prefix.>`, and a
  non-owner's `Start` still checks that its stream does. `Publish` refuses a type that is not
  under the prefix, the prefix itself included, as `event.Permanent`, naming the type and the
  prefix, so a relay quarantines its row; `Subscribe` refuses a subscription naming such a type.
  An event a v0.1.0 broker published on `Prefix.<type>` reaches a v0.2.0 consumer only when its
  type is under the prefix, and with its type unchanged.
- `natstest.Scratch`'s prefix is the test's type namespace: a test's event types are under it.

## [v0.1.0] - 2026-10-13

### Added

- `Broker`, the NATS JetStream provider of `messaging.Broker` on one stream, and `New`, which
  checks its config and does no I/O. `Start` connects, bounded by its context's deadline or 10s,
  and reconnects without limit once up; a call before Start fails with `ErrNotStarted`, and a
  second Start with `ErrStarted`. A failed Start closes what it opened. `Ready` reports a
  started broker whose connection is up, `Shutdown` drains the connection, and `Conn` is the
  native handle.
- Stream ownership: the broker `Owner` marks creates the stream, or updates it to its config,
  with file storage, limits retention, the subjects `Prefix.>`, the deduplication window, and
  `MaxAge`. Every other broker writes nothing and binds the stream as it finds it, failing to
  start when the stream is missing or none of its subjects captures the prefix.
- `Publish`, which sends an event in binary content mode to `Prefix.<type>` with its
  length-prefixed source and id as `Nats-Msg-Id`, so the stream's window drops a repeat, and
  returns once the stream holds it. It rejects what `messaging.Encode` rejects and a header value
  holding CR or LF.
- `Subscribe`, whose source binds its durable consumer at its first Receive on a config derived
  from the normalized subscription, its AckWait the subscription's plus `AckMargin`, and pulls
  one message at a time. The handler's context carries the delivery's attempt and a deadline of
  the subscription's AckWait; an outcome settled after it is dropped. The source reports each
  error it survives: a message that does not decode or whose metadata does not parse, both
  terminated; an outcome that fails to send; and every failed pull, including one that ends
  empty while the connection is down, after which the source is not ready until a pull
  succeeds. A closed connection, or a deleted consumer or stream, ends Receive.
- `Config`, the service's nats block: `URL` (`DefaultURL`), `Name`, `Stream`, `Prefix`, `Owner`,
  `Duplicates`, and `MaxAge`, with `Merge`, `Finalize`, which reads the `<PREFIX>_NATS_*`
  environment overrides, and `Validate`, which rejects a non-owner that sets `Duplicates` or
  `MaxAge`, naming the field.
- `natstest`, which gives each test its own stream: `Open` and `Scratch`.
- The provider passes `messagingtest.Run`. Its integration suite runs against a compose NATS
  (`nats:2.15.1-alpine`, JetStream on tmpfs), through `mise run integration:nats`.
- The module requires `go-messaging` v0.1.0, go-core v0.10.0, and nats.go v1.54.0.

[Unreleased]: https://github.com/standards-lab/go-messaging/compare/nats/v0.2.0...HEAD
[v0.2.0]: https://github.com/standards-lab/go-messaging/compare/nats/v0.1.0...nats/v0.2.0
[v0.1.0]: https://github.com/standards-lab/go-messaging/releases/tag/nats/v0.1.0
