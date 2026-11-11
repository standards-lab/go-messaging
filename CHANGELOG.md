# Changelog

All notable changes to `github.com/standards-lab/go-messaging` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the module adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). This changelog covers the base module
only; the `postgres` and `nats` sub-modules keep their own.

## [Unreleased]

## [v0.3.0] - 2026-11-11

Configuration is one value, not variadic option functions or a long parameter list: a service's
runtime is built with `messaging.Setup{...}.Build()`, and `Outbox.Relay` takes an
`outbox.RelayOptions`. The `go-core` requirement moves to v0.12.0 and the `sqlate` requirement to
v0.7.0, which make the same change to their own constructors and transactions.

### Added

- `Setup`, what a service's `Runtime` is built from: `Config`, `Broker`, `Outbox`, `Inbox`,
  `Shutdown`, and `Logger`, all required, and `OnFailure` and `Gate`, optional. `Setup.Build`
  builds the `Runtime` that `New` built. It panics, before any other check, with
  `messaging: Setup.Broker is nil` or `messaging: Setup.Logger is nil`, and returns `New`'s
  errors for a `Config` that does not validate, a `Shutdown` that is not positive, or an engine
  missing a statement; it does no I/O. A nil `OnFailure` keeps the default hook, which logs each
  failure at warn, and a nil `Gate` holds none; a non-nil `Gate` holding a nil pointer is a set
  gate, and is held.
- `outbox.RelayOptions`, which configures a `Relay`. `Report` gets each error the relay survives,
  `Timeout` bounds each row's handler, `Drain` bounds the last pass after cancellation, and
  `Gate` is held shared for each row. The zero value discards reports, bounds each handler at
  10s, makes no last pass, and holds no gate.

### Changed

- **Breaking:** The `go-core` requirement is v0.12.0, which pulls go-core's v0.11.0 and v0.12.0
  breaks into an importer's build. `reactor.New` takes a trailing `reactor.Options` and
  `event.NewRecorder` an `event.Options` in place of variadic option values,
  `reactor.CancelWithin` returns a `time.Duration` for `reactor.Options.CancelAfter`, and
  `reactor.Option`, `reactor.CancelAfter`, `event.Option`, `event.Clock`, and `event.IDs` are
  removed. From v0.11.0, `graph.Scope.After` is removed, `graph.Graph.Observe` takes a
  `func(graph.Ref)`, `graph.Dependency` gains a `Ref` field, and a `Monitored` value's runtime
  failure ends the run labelled with its node's name. go-core's changelog migrates each. The
  runtime's reactors cancel their handlers after the same bound as before, now set as
  `reactor.Options.CancelAfter`.
- **Breaking:** The `sqlate` requirement is v0.7.0. `DB.Begin`, `DB.Transact`, and `Transact`
  take a trailing `*sql.TxOptions` in place of variadic `TxOption` values, and
  `sqlate.Beginner.Begin` has `DB.Begin`'s new signature, so a type that implements `Beginner`
  with its own `Begin` to hand to `Runtime.Relay` or `Outbox.Relay` changes its signature; one
  embedding `*sqlate.DB` is unaffected. sqlate's changelog migrates each. The relay opens its
  transactions with nil, a default transaction, as before.
- **Breaking:** `outbox.Outbox.Relay(db, poll, opts)` takes an `outbox.RelayOptions` in place of
  its `report` parameter and variadic `RelayOption` values; the report moves into
  `RelayOptions.Report`. A call with neither passes `outbox.RelayOptions{}`.
- `outbox.Outbox.Relay` accepts a zero `Timeout` as the 10s default, where a zero `outbox.Timeout`
  panicked. It panics on a poll that is not positive or a negative `Timeout` or `Drain`, with the
  message `outbox: relay poll must be positive, and timeout and drain not negative`.

### Removed

- **Breaking:** `New`, with no alias. Build a `Setup` and call its `Build`.
- **Breaking:** `Option` and the `OnFailure` and `Gated` options, with no aliases. Set
  `Setup.OnFailure` and `Setup.Gate`.
- **Breaking:** `outbox.RelayOption` and the `outbox.Timeout`, `outbox.Drain`, and
  `outbox.Gated` option functions, with no aliases. Set `RelayOptions.Timeout`,
  `RelayOptions.Drain`, and `RelayOptions.Gate`.

### Migrating from v0.2.0

- **`New`'s parameters and options become one `messaging.Setup` value.** Each positional
  parameter is a field of the same meaning, and each option a field, nil for its default.

  ```go
  // v0.2.0
  rt, err := messaging.New(cfg, broker, postgres.Outbox(), postgres.Inbox(), shutdown, logger)
  rt, err := messaging.New(cfg, broker, postgres.Outbox(), postgres.Inbox(), shutdown, logger,
  	messaging.OnFailure(hook), messaging.Gated(&gate))

  // v0.3.0
  rt, err := messaging.Setup{
  	Config: cfg, Broker: broker, Outbox: postgres.Outbox(), Inbox: postgres.Inbox(),
  	Shutdown: shutdown, Logger: logger,
  }.Build()
  rt, err := messaging.Setup{
  	Config: cfg, Broker: broker, Outbox: postgres.Outbox(), Inbox: postgres.Inbox(),
  	Shutdown: shutdown, Logger: logger,
  	OnFailure: hook, Gate: &gate,
  }.Build()
  ```

- **`Build` panics on a nil `Broker` or `Logger`.** `New` accepted either; `Build` panics naming
  the field, before it checks anything else, since a nil one is a wiring mistake rather than a
  configuration error. Its returned errors are `New`'s.

- **A relay's report and options become one `outbox.RelayOptions` value.** The report moves
  from its own parameter into `Report`; leave a field unset for its default.

  ```go
  // v0.2.0
  relay := ob.Relay(db, poll, nil)
  relay := ob.Relay(db, poll, report, outbox.Timeout(5*time.Second), outbox.Drain(drain),
  	outbox.Gated(&gate))

  // v0.3.0
  relay := ob.Relay(db, poll, outbox.RelayOptions{})
  relay := ob.Relay(db, poll, outbox.RelayOptions{
  	Report: report, Timeout: 5 * time.Second, Drain: drain, Gate: &gate,
  })
  ```

- **A zero `Timeout` is the 10s default.** `outbox.Timeout(0)` panicked; `RelayOptions.Timeout`
  left zero bounds each row's handler at 10s, as no `Timeout` option did. A negative `Timeout`
  still panics.

- **go-core's and sqlate's own calls change too.** go-core's changelog migrates `reactor.New`
  to `reactor.Options`, `reactor.CancelWithin`, and `event.NewRecorder` to `event.Options` from
  v0.11.0, and the graph changes from v0.10.0. sqlate's changelog migrates `DB.Begin`,
  `DB.Transact`, `Transact`, and a `Beginner`'s `Begin` to a trailing `*sql.TxOptions` from
  v0.6.x, so a consumer's `sqlate.Transact(ctx, pool, fn)` becomes
  `sqlate.Transact(ctx, pool, fn, nil)`.

## [v0.2.0] - 2026-10-14

### Added

- `Gated`, the `Option` that sets a `Gate`, a quiesce gate such as go-core's `reactor.Gate`, which
  the runtime's reactors hold shared. The relay holds it for each outbox row, from before the
  row's transaction begins until it commits or rolls back, so a relay waiting on it opens no
  transaction; a wait the stop or the drain ends claims no row and reports nothing, and any other
  failure of the gate is reported as a `FailureRelay`. Each delivery a `Consume` reactor handles
  holds it for the handler's whole call; a wait the drain ends hands the handler nothing and
  leaves the event for redelivery, and a delivery that waits past `AckWait` is redelivered and
  counts against `MaxDeliver`. Without `Gated`, the relay and the consumers hold no gate, as in
  v0.1.0.
- `outbox.Gate` and the `outbox.Gated` relay option, the relay's side of `Gated`.

### Changed

- Documentation only: the package documentation names the `nats` sub-module as the NATS
  JetStream provider rather than a reserved one, and `OnFailure`'s names the failure volume the
  nats provider's sources report during an outage.

## [v0.1.0] - 2026-10-13

### Added

- The broker contract: `Publisher` and `Broker`, whose `Subscribe` returns a go-core
  `reactor.Source` of a durable consumer's events. The handler's return is the delivery's
  outcome: nil acknowledges, an error redelivers after `RetryDelay` up to `MaxDeliver`
  deliveries, and an `event.Permanent` error terminates. A broker deduplicates on the event's
  source and id within its window, `DefaultDuplicates` when it sets none. `Encode` is the check
  and encoding every provider's `Publish` makes.
- `Subscription`: a durable consumer's `Name`, its type filter `Types`, `MaxDeliver`, `AckWait`
  (`DefaultAckWait` when unset), `RetryDelay`, and `Start` (`StartAll` or `StartNew`), with
  `Validate`, `Normalize`, and `Matches`. `CheckName` and `CheckType` are the rules for a
  consumer's name and a routable event type.
- `WithAttempt` and `Attempt`: each delivery's handler context carries its attempt, 1 on the
  first delivery and one more on each redelivery.
- `Report`, which receives the errors a source survives.
- `outbox`: `New` over an `Engine` of compiled statements; `Outbox.Sink`, the `event.Sink` a
  recorder writes a command's events through in the command's own transaction; and the `Relay`,
  which publishes each committed row in a transaction of its own, with the `Timeout` and `Drain`
  options. A row that does not decode, or whose publish fails with an `event.Permanent` error,
  is quarantined in its own transaction and reported as a `QuarantineError`, so it does not hold
  the outbox back; a transient publish failure is reported as a `PublishError` and retried on
  the next pass.
- `inbox`: `New` over an `Engine`, and `Inbox.Claim`, which records a consumer's handling of an
  event in the handler's transaction and reports whether it is the first.
- `Config`, the service's messaging block: `Source`, `RelayPoll` (`DefaultRelayPoll`), and
  `RelayTimeout` (`DefaultRelayTimeout`), the bound on the relay's publish of one event, with
  `Merge`, `Finalize`, which reads the `<PREFIX>_MESSAGING_*` environment overrides, and
  `Validate`.
- `Runtime`, built by `New` from the config, a broker, and an engine's outbox and inbox
  statements: its `Recorder` for a service's domains, `Relay`, the reactor that publishes the
  outbox and makes a last pass on drain, and `Consume`, which builds a consuming reactor that
  decodes each event into the consumer's type and hands its handler a `Claim` bound to the
  event. Both log the traffic they carry.
- The error hook: each failure the relay and the consumers survive reaches the `Hook` that
  `OnFailure` sets as a `Failure` of kind `FailureRelay`, `FailureQuarantine`, `FailureSource`,
  or `FailureExhausted`, the last on the handler's failure on attempt `MaxDeliver`. Without a
  hook, each failure is logged at warn. An event whose last delivery overruns `AckWait` is given
  up unreported.
- `memory`, the in-memory provider, and `messagingtest`, the conformance suite every provider
  passes.
- The base module depends on the standard library, go-core, and sqlate and sqlate/query, which
  depguard enforces.

[Unreleased]: https://github.com/standards-lab/go-messaging/compare/v0.3.0...HEAD
[v0.3.0]: https://github.com/standards-lab/go-messaging/compare/v0.2.0...v0.3.0
[v0.2.0]: https://github.com/standards-lab/go-messaging/compare/v0.1.0...v0.2.0
[v0.1.0]: https://github.com/standards-lab/go-messaging/releases/tag/v0.1.0
