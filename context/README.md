# go-messaging

go-messaging is the messaging infrastructure library of Go Elemental, the Standards Lab
organization's Go implementation of the Elemental Architecture. It gives a service one
broker-agnostic contract to emit events and run reactors on: events are go-core's CloudEvents
`event` type, emitted through a transactional outbox and consumed idempotently through an inbox,
and consumers are go-core `reactor`s on a subscription source. NATS JetStream is the production
provider, and a memory provider serves tests. The base module depends on the standard library,
go-core, and sqlate with its `query` package, the adjacent library the outbox's and inbox's
engine contract builds on; the postgres engine and the NATS provider are sub-modules.

The README, the pages under `docs/`, and each package's `doc.go` document this repository. The
[Go Elemental](https://github.com/standards-lab/architecture/blob/main/standards/go-elemental/README.md) standard states the principles it follows.
This context records only working knowledge the code and its documentation do not express.

The design is the coordinator's `standards-lab/context/messaging.md`.

The code and each `doc.go` are authoritative for a built capability; an unbuilt capability gains
written detail when a session is about to build it.

## Capability map

- **Broker contract and subscriptions** (built): the root package, `messaging`
  ([doc.go](../doc.go), [broker.go](../broker.go)).
- **Runtime and its error hook** (built): the root package ([runtime.go](../runtime.go),
  [config.go](../config.go), [failure.go](../failure.go)).
- **Outbox** (built): the outbox, with poison-row quarantine ([outbox/doc.go](../outbox/doc.go)).
- **Inbox** (built): [inbox/doc.go](../inbox/doc.go).
- **Memory provider** (built): the memory provider and the conformance suite
  ([memory/doc.go](../memory/doc.go), [messagingtest/doc.go](../messagingtest/doc.go)).
- **Postgres engine** (built): the engine, its `messaging` migration set, and the outbox and
  inbox retentions ([postgres/doc.go](../postgres/doc.go) and
  [docs/usage.md, "Retention"](../docs/usage.md#retention)). Its integration suite makes each
  test's database through `github.com/standards-lab/sqlate/postgres/pgtest`.
- **NATS provider** (built): the provider, with stream ownership ([nats/doc.go](../nats/doc.go)),
  and the scratch-stream helper ([nats/natstest/doc.go](../nats/natstest/doc.go)).
- **CI and release** (built): [.github/workflows/](../.github/workflows/) and the README's
  [Releases](../README.md#releases) section.
