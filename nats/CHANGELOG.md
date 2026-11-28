# Changelog

All notable changes to the NATS JetStream provider (`github.com/standards-lab/go-messaging/nats`)
are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). This
changelog covers this sub-module only; the base module keeps its own.

## [Unreleased]

## [v0.5.0] - 2026-11-27

### Added

- `Queue`, with `NewQueue` and its `QueueConfig` block: a `messaging.Queue` on a stream with
  work-queue retention, a lifecycle component that does no I/O until `Start`. The owner creates
  or updates the stream on file storage over the work subject, the dead-letter subject, and every
  subject under it; a non-owner binds the stream as it finds it. `Enqueue` stores an event on the
  work subject with its source and id as `Nats-Msg-Id`. The workers under one name share one
  durable pull consumer with no server delivery limit, and a work-queue stream takes one worker
  group per subject. Each worker holds one job at a time, extends its delivery while the handler
  runs, ends the handler's context when no extension is answered for AckWait, acknowledges a
  nil, and naks an error with the attempt's retry delay. A permanent error, an error on the
  final attempt, any delivery past it, which the worker dead-letters without running the
  handler, and a message that does not decode dead-letter the job: the message, unchanged but
  for the dead-letter headers, is published on the worker's dead-letter subject with a
  `Nats-Msg-Id` of its own, so it is stored once, and the job is acknowledged only after the
  stream holds it. A failed publish naks the job, and its next delivery is its next attempt:
  past the final attempt it dead-letters again without the handler, and before it the handler
  runs again, so the job always ends as a dead letter. `Subscribe` rejects a worker whose
  AckWait plus `AckMargin` is not less than `messaging.DefaultDuplicates`, so a dead letter
  repeated after a lost acknowledgement falls within the deduplication window; AckWait bounds a
  delivery between extensions, not a job's run time. A worker on a dead-letter subject puts each
  dead letter's `messaging.DeadLetter` on the handler's context. The provider passes
  `messagingtest.RunQueue`.
- `natstest.ScratchQueue`, a stream and a work queue's subjects unique to the test.

### Changed

- `ErrNotStarted` and `ErrStarted` also name a `Queue` that is not started, or started twice.
- Requires go-messaging v0.5.0, for the work-queue contract.

## [v0.4.0] - 2026-11-27

### Added

- `Broker.Append` and `Broker.Read`, implementing `messaging.Journal` on the broker's stream.
  `Append` publishes the event on the caller's subject, which must be under `Prefix`, with the
  subject's expected last sequence and the event's source and id as `Nats-Msg-Id`. It reports a
  wrong last sequence as `messaging.ErrConflict` unless the subject's last message is the
  appended event. `Read` reads the subject through an ordered consumer, bounded by the
  subject's last message when the read begins, and deletes the consumer when the read ends.
- The provider passes `messagingtest.RunJournal`.

### Changed

- The `go-messaging` requirement is v0.4.0, which adds `messaging.Journal`.
- nats.go is v1.56.0.

## [v0.3.0] - 2026-11-11

### Changed

- **Breaking:** The `go-messaging` requirement is v0.3.0, which builds a service's runtime with
  `messaging.Setup{...}.Build()` in place of `messaging.New` and its `Option`, `OnFailure`, and
  `Gated`, and takes `outbox.Outbox.Relay`'s report and options as one `outbox.RelayOptions`
  in place of its `report` parameter and `RelayOption`, `outbox.Timeout`, `outbox.Drain`, and
  `outbox.Gated`. The base module's v0.3.0 entry migrates each. It takes in v0.2.0's quiesce
  gate, now set as `Setup.Gate` and `RelayOptions.Gate`. The broker and its sources are
  unchanged.
- **Breaking:** The `go-core` requirement is v0.12.0, whose `reactor.New` and
  `event.NewRecorder` take a trailing Options struct in place of variadic option functions, and
  which pulls go-core v0.11.0's graph changes into an importer's build. go-core's changelog
  migrates each.
- nats.go is v1.55.0.

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

[Unreleased]: https://github.com/standards-lab/go-messaging/compare/nats/v0.5.0...HEAD
[v0.5.0]: https://github.com/standards-lab/go-messaging/compare/nats/v0.4.0...nats/v0.5.0
[v0.4.0]: https://github.com/standards-lab/go-messaging/compare/nats/v0.3.0...nats/v0.4.0
[v0.3.0]: https://github.com/standards-lab/go-messaging/compare/nats/v0.2.0...nats/v0.3.0
[v0.2.0]: https://github.com/standards-lab/go-messaging/compare/nats/v0.1.0...nats/v0.2.0
[v0.1.0]: https://github.com/standards-lab/go-messaging/releases/tag/nats/v0.1.0
