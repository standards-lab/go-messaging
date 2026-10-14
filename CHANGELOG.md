# Changelog

All notable changes to `github.com/standards-lab/go-messaging` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the module adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). This changelog covers the base module
only; the `postgres` and `nats` sub-modules keep their own.

## [Unreleased]

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

[Unreleased]: https://github.com/standards-lab/go-messaging/compare/v0.1.0...HEAD
[v0.1.0]: https://github.com/standards-lab/go-messaging/releases/tag/v0.1.0
