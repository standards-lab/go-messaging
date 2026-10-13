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

The base module depends on the standard library, go-core, sqlate, and sqlate/query, the adjacent
library the outbox and inbox engine contract builds on, and nothing else: no driver, no dialect, no
broker client. depguard holds the base to that line in `mise run check`. An engine's driver and
dialect enter only through its sub-module, and a broker's client only through its provider's.

The `nats` sub-module is a scaffold: the JetStream provider is not built yet.

## Packages

- `messaging` (the root) is the broker contract and the runtime: `Broker` and `Publisher`,
  `Subscription` and its `Start`, the delivery `Attempt`, the source's `Report`, `Config`, the
  `Runtime` with its `Relay` and `Consume` reactors, and the error hook, `Failure`, `Hook`, and
  `OnFailure`.
- `outbox` writes a command's events in the command's own transaction and relays them to the
  broker, quarantining a row that can never be published.
- `inbox` is a consumer's claim on an event, so a redelivery is handled once.
- `memory` is the in-memory provider, for a service's tests and the conformance double.
- `messagingtest` is the conformance suite, `Run`, every provider passes.
- `postgres` (sub-module) is the PostgreSQL engine: the outbox's and inbox's statements, the
  `messaging` migration set, `Verify`, and `Pending`. Its `pgtest` package gives each test a
  throwaway database.
- `nats` (sub-module) is reserved for the NATS JetStream provider.

The package documentation, `go doc github.com/standards-lab/go-messaging/...`, is the reference
for each exported name; each package's `doc.go` inventories its exports.

## Usage

### Wiring the runtime

A service builds one `Runtime` from its messaging `Config`, a broker, and its engine's statements.
The base module imports neither: the composition root hands them over. Declare the engine's
migration set ahead of the service's own, and call `postgres.Verify` from the verify stage.

```go
cfg := messaging.Config{Source: "/billing"}
err := cfg.Finalize("BILLING") // defaults relay_poll and relay_timeout, reads BILLING_MESSAGING_*

set, err := postgres.Migrations() // the messaging set: messaging_outbox and messaging_inbox

rt, err := messaging.New(cfg, broker, postgres.Outbox(), postgres.Inbox(), shutdown, logger)
```

`broker` is any `messaging.Broker`: `memory.New()` in a test, and in production the NATS JetStream
provider once the `nats` sub-module, a scaffold today, delivers it.
`shutdown` is the process's drain timeout, within which every reactor the runtime builds drains.
Error handling is elided throughout.

A domain emits through `rt.Recorder`, in its command's own transaction, so an event is written
exactly when the state it reports commits:

```go
_, err := sqlate.Transact(ctx, db, rt.Recorder.Emit(ctx, func(tx *sqlate.Tx, q *event.Queue) (struct{}, error) {
	// ... the command's writes on tx ...
	InvoiceIssued.Raise(q, invoiceID, InvoiceIssuedData{Amount: amount})
	return struct{}{}, nil
}))
```

### The relay and consumers as graph nodes

The runtime registers nothing. The composition root makes the broker and each reactor a node of
its go-core graph, and the coordinator starts, drains, and watches them from their methods. In
the sketch below, `runtime` and `db` are the service's nodes for the `*messaging.Runtime` and its
`*sqlate.DB`.

`rt.Relay(db)` is the reactor that publishes the committed outbox rows. Every node that commits
events uses the relay's node, so it drains before the relay, whose last pass publishes what was
committed during the drain.

`rt.Consume` builds a consuming reactor. It decodes each event's data into the consumer's own
type and hands its handler a `Claim` over the inbox, bound to the event and to the subscription's
`Name`. The handler runs the claim first in its own transaction and commits nothing more when it
is not the first:

```go
relay := g.Define("relay", func(s *graph.Scope) (*reactor.Reactor[event.Event], error) {
	return s.Use(runtime).Relay(s.Use(db)), nil
})

ledger := g.Define("ledger", func(s *graph.Scope) (*reactor.Reactor[event.Event], error) {
	pool := s.Use(db) // a Use runs in the constructor, never in the handler
	sub := messaging.Subscription{Name: "ledger", Types: []string{"billing.invoice.issued"}, MaxDeliver: 5}
	return s.Use(runtime).Consume(sub, func(ctx context.Context, d InvoiceIssuedData, claim messaging.Claim) error {
		_, err := sqlate.Transact(ctx, pool, func(tx *sqlate.Tx) (struct{}, error) {
			if first, err := claim(ctx, tx); err != nil || !first {
				return struct{}{}, err
			}
			// ... the consumer's work on tx ...
			return struct{}{}, nil
		})
		return err
	})
})
```

The handler's return is the delivery's outcome: nil acknowledges it, an error redelivers it after
`RetryDelay`, and an error wrapped with `event.Permanent` terminates it. Data that does not decode
is refused permanently.

### The error hook

The failures the relay and the consumers survive, which no caller sees as an error, go to the
runtime's hook, each as a `messaging.Failure` whose `Kind` names what failed: `relay` (a pass
ended by a transient publish failure or a database error), `quarantine`, `source` (an error a
consumer's source survived), or `exhausted`. Without a hook, the runtime logs each failure at warn
as `messaging failure`. The default logs every failure it is given, about four a second during an
outage at the default poll, so a service that wants failures throttled, counted, or alerted on
sets its own:

```go
rt, err := messaging.New(cfg, broker, postgres.Outbox(), postgres.Inbox(), shutdown, logger,
	messaging.OnFailure(func(ctx context.Context, f messaging.Failure) {
		failures.WithLabelValues(string(f.Kind)).Inc()
	}))
```

A hook must not block for long: the relay and the consumer wait on it.

### Quarantine

An outbox row that can never be published, one whose event does not decode or whose publish
failed with an `event.Permanent` error, is quarantined in its own transaction, so it does not
hold the outbox back. The relay reports it as a `quarantine` failure carrying an
`*outbox.QuarantineError`. The row stays in `messaging_outbox` with `quarantined_at` and
`quarantine_reason` set; the relay never claims it again and `postgres.Pending` does not count it.

To inspect the quarantined rows:

```sql
SELECT seq, source, id, header, quarantined_at, quarantine_reason
FROM messaging_outbox
WHERE quarantined_at IS NOT NULL
ORDER BY seq;
```

To release a row once its cause is fixed, clear both columns together, which the table's check
constraint requires; the relay claims it on its next pass:

```sql
UPDATE messaging_outbox
SET quarantined_at = NULL, quarantine_reason = NULL
WHERE seq = $1;
```

### MaxDeliver exhaustion

A subscription's `MaxDeliver` bounds the deliveries of one event; 0 is unlimited. A handler reads
which delivery it is on with `messaging.Attempt(ctx)`. When the handler fails, with an error not
marked permanent, on the delivery whose attempt is `MaxDeliver`, the broker gives the event up and
the runtime reports an `exhausted` failure with the event's identity and the attempt.

Exhaustion is seen only from the handler's error. An event whose last delivery overruns
`AckWait`, with the handler returning nil or returning late, is given up unreported. Size
`AckWait` above the handler's worst case where an unreported loss matters.

## Development

The repository is a Go workspace of three modules, `.`, `postgres`, and `nats`. Tasks run through
[mise](https://mise.jdx.dev):

```sh
mise run check         # build, vet, format, fix, tidy, test, and lint every module, then sqlint; writes nothing
mise run integration   # the postgres module's integration suite against a fresh compose PostgreSQL
mise run currency      # report requirements, Go, tools, actions, and images behind their latest
mise run upgrade       # upgrade every module's go directive and requirements, and the tools, to their latest
mise run up            # start the compose PostgreSQL and wait until it is healthy
mise run down          # stop it, discarding its data
```

The check builds each module with the workspace off, so each resolves its own pins. `compose.yml`
runs PostgreSQL on port 5435; `POSTGRES_PORT` moves it, and `MESSAGING_DSN` in `mise.toml`
follows. The integration suite keeps its data in tmpfs and gives each test its own database. CI
runs the check on every pull request and push to main, and the integration suite on push to main
and on manual dispatch.

## Releases

Each module is versioned and released on its own tags: `v*` for the base module, `postgres/v*`
for the engine, and `nats/v*` for the provider. A pushed tag creates the module's GitHub release
from the matching section of its changelog: [`CHANGELOG.md`](CHANGELOG.md),
[`postgres/CHANGELOG.md`](postgres/CHANGELOG.md), and [`nats/CHANGELOG.md`](nats/CHANGELOG.md).

## License

[Apache License 2.0](LICENSE).
