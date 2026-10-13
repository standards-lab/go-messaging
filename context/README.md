# go-messaging

go-messaging is the messaging infrastructure library of Go Elemental, the Standards Lab
organization's Go implementation of the Elemental Architecture. It gives a service one
broker-agnostic contract to emit events and run reactors on: events are go-core's CloudEvents
`event` type, emitted through a transactional outbox and consumed idempotently through an inbox,
and consumers are go-core `reactor`s on a subscription source. NATS JetStream is the declared
provider, and a memory provider serves tests. The base module depends on the standard library and
go-core only; the postgres engine and the NATS provider are sub-modules.

The README and each package's `doc.go` document this repository. The
[Go Elemental](https://github.com/standards-lab/architecture/blob/main/standards/go-elemental/README.md) standard states the principles it follows.
This context records only working knowledge the code and the README do not express.

go-messaging is the root of the `v1.messaging` goal, whose record is `context/goals/v1.messaging.md`.
The design is the coordinator's `standards-lab/context/messaging.md`. The source is
spike-messaging's `messaging` modules
([github.com/JaimeStill/spike-messaging](https://github.com/JaimeStill/spike-messaging), archived).

## Capability map

Nothing is built yet. The code and `doc.go` become authoritative as each capability lands, and
an unbuilt capability gains written detail when a session is about to build it.

- **Broker contract and subscriptions**: `Subscription` with `Start`, `MaxDeliver`, and `Normalize`.
- **Runtime and its error hook**: `Config` with the relay `Timeout`, and the delivery attempt.
- **Outbox**: transactional emission, with poison-row quarantine.
- **Inbox**: idempotent consumption.
- **Memory provider** and **`messagingtest`**, the conformance suite.
- **Postgres engine**: the `postgres` sub-module and its `messaging` migration set.
- **NATS provider**: the JetStream `nats` sub-module, binding a stream without provisioning it.
- **CI and release**: CI runs `mise run check` on every pull request; each module releases on its own tags.
