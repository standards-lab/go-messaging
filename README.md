# go-messaging

The messaging infrastructure library of Go Elemental: one broker-agnostic contract for a service
to emit events and run reactors on. Events are go-core's CloudEvents `event` type, emitted through
a transactional outbox and consumed idempotently through an inbox, and every consumer is a go-core
`reactor` on a subscription source.

## Modules

| Module | Import path | Tags |
| --- | --- | --- |
| Base | `github.com/standards-lab/go-messaging` | `v*` |
| PostgreSQL engine | `github.com/standards-lab/go-messaging/postgres` | `postgres/v*` |
| NATS JetStream provider | `github.com/standards-lab/go-messaging/nats` | `nats/v*` |

The base module depends on the standard library, go-core, and sqlate with its `query` package,
the adjacent library the outbox's and inbox's engine contract builds on, and nothing else: no
driver, no dialect, no broker client. depguard holds the base to that line in `mise run check`. An
engine's driver and dialect enter only through its sub-module, and a broker's client only through
its provider's.

## Packages

- `messaging` (the root) is the broker contract and the runtime: `Broker` and `Publisher`,
  `Subscription` and its `Start`, the delivery `Attempt`, the source's `Report`, `Config`, the
  `Runtime` with its `Relay` and `Consume` reactors, the `Setup` whose `Build` builds it, the
  error hook's `Failure` and `Hook`, and the quiesce gate's `Gate`. Its `Journal`, with `Entry`
  and `ErrConflict`, appends to a subject of the broker's stream and reads the subject back.
- `outbox` writes a command's events in the command's own transaction and relays them to the
  broker, quarantining a row that can never be published. `Outbox.Relay` takes its report,
  per-row timeout, last-pass drain, and gate as one `outbox.RelayOptions` value.
- `inbox` is a consumer's claim on an event, so a redelivery is handled once.
- `memory` is the in-memory provider, a `Broker` and a `Journal`, for a service's tests and the
  conformance double.
- `messagingtest` is the conformance suite every provider passes: `Run` for the broker, and
  `RunJournal` for the journal.
- `postgres` (sub-module) is the PostgreSQL engine: the outbox's and inbox's statements, the
  `messaging` migration set, `Verify`, `Pending`, and the retentions, `OutboxRetention` and
  `InboxRetention`, that delete aged published and handled rows a budget at a time.
- `nats` (sub-module) is the NATS JetStream provider: the `Broker` on one stream, which is also
  a `Journal`, its `Config` block with the stream's owner switch, and the durable pull consumers
  its sources bind. Its `natstest` package gives each test its own stream.

The package documentation, `go doc github.com/standards-lab/go-messaging/...`, is the reference
for each exported name; each package's `doc.go` inventories its exports.

## Usage

[`docs/usage.md`](docs/usage.md) shows the library in use: wiring the runtime, the NATS broker,
the relay and consumers as graph nodes, the error hook, quarantine, retention, MaxDeliver
exhaustion, the quiesce gate, and folding and appending to a stream-backed root.

## Development

The repository is a Go workspace of three modules, `.`, `postgres`, and `nats`. Tasks run through
[mise](https://mise.jdx.dev):

```sh
mise run check                 # build, vet, format, fix, tidy, test, and lint every module, then sqlint, lint-context, and lint-references; writes nothing
mise run integration:postgres  # the postgres module's integration suite against a fresh compose PostgreSQL
mise run integration:nats      # the nats module's integration suite against a fresh compose NATS
mise run integration           # both suites, each against its own fresh service
mise run currency              # report requirements, Go, tools, actions, and images behind their latest
mise run upgrade               # upgrade every module's go directive and requirements, and the tools, to their latest
mise run up                    # start every compose service and wait until each is healthy
mise run down                  # stop them, discarding their data
```

The check builds each module with the workspace off, so each resolves its own pins. `compose.yml`
runs PostgreSQL on port 5435 and NATS with JetStream on port 4225, clear of the other repositories'
development ports. `POSTGRES_PORT` and `NATS_PORT` move them, and `MESSAGING_DSN` and
`MESSAGING_NATS_URL` in `mise.toml` follow. Each integration task starts only its own service and
stops it whatever the result. Both services keep their data in tmpfs; the postgres suite gives each
test its own database, and the nats suite its own stream. CI runs the check on every pull request
and push to main, and both integration suites on push to main and on manual dispatch.

[`docs/internals.md`](docs/internals.md) holds the reasoning behind the unexported machinery of
each package, for a contributor changing it.

## Releases

Each module is versioned and released on its own tags: `v*` for the base module, `postgres/v*`
for the engine, and `nats/v*` for the provider. A pushed tag creates the module's GitHub release
from the matching section of its changelog: [`CHANGELOG.md`](CHANGELOG.md),
[`postgres/CHANGELOG.md`](postgres/CHANGELOG.md), and [`nats/CHANGELOG.md`](nats/CHANGELOG.md).

## License

[Apache License 2.0](LICENSE).
