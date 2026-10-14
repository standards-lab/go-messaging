# go-messaging

go-messaging is the messaging infrastructure library of Go Elemental, the Standards Lab
organization's Go implementation of the Elemental Architecture. It gives a service one
broker-agnostic contract to emit events and run reactors on: events are go-core's CloudEvents
`event` type, emitted through a transactional outbox and consumed idempotently through an inbox,
and consumers are go-core `reactor`s on a subscription source. NATS JetStream is the production
provider, and a memory provider serves tests. The base module depends on the standard library,
go-core, and sqlate with its `query` package, the adjacent library the outbox's and inbox's
engine contract builds on; the postgres engine and the NATS provider are sub-modules.

The README and each package's `doc.go` document this repository. The
[Go Elemental](https://github.com/standards-lab/architecture/blob/main/standards/go-elemental/README.md) standard states the principles it follows.
This context records only working knowledge the code and the README do not express.

go-messaging is the root of the `v1.messaging` goal, whose record is `context/goals/v1.messaging.md`.
The design is the coordinator's `standards-lab/context/messaging.md`. The source is
spike-messaging's `messaging` modules
([github.com/JaimeStill/spike-messaging](https://github.com/JaimeStill/spike-messaging), archived).

## Capability map

The code and each `doc.go` are authoritative for a built capability; an unbuilt capability gains
written detail when a session is about to build it.

- **Broker contract and subscriptions**: the root package, `messaging` (`doc.go`, `broker.go`).
- **Runtime and its error hook**: the root package (`runtime.go`, `config.go`, `failure.go`).
- **Outbox**, with poison-row quarantine: `outbox/doc.go`.
- **Inbox**: `inbox/doc.go`.
- **Memory provider** and the conformance suite: `memory/doc.go`, `messagingtest/doc.go`.
- **Postgres engine** and its `messaging` migration set: `postgres/doc.go`, with the
  scratch-database helper in `postgres/pgtest/doc.go`.
- **NATS provider**, with stream ownership: `nats/doc.go`, with the scratch-stream helper in
  `nats/natstest/doc.go`.
- **CI and release**: `.github/workflows/`; the README's "Releases" section.
