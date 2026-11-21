# Usage

How a service wires and runs go-messaging. Each package's `doc.go` and the
[README](../README.md) state what the packages promise; this page shows them in use.

## Wiring the runtime

A service builds one `Runtime` with a `messaging.Setup`: its messaging `Config`, a broker, and its
engine's statements. The base module imports neither: the composition root hands them over.
Declare the engine's migration set ahead of the service's own, and call `postgres.Verify` from the
verify stage.

```go
cfg := messaging.Config{Source: "/billing"}
err := cfg.Finalize("BILLING") // defaults relay_poll and relay_timeout, reads BILLING_MESSAGING_*

set, err := postgres.Migrations() // the messaging set: messaging_outbox and messaging_inbox

rt, err := messaging.Setup{
	Config:   cfg,
	Broker:   broker,
	Outbox:   postgres.Outbox(),
	Inbox:    postgres.Inbox(),
	Shutdown: shutdown,
	Logger:   logger,
}.Build()
```

`Broker` is any `messaging.Broker`: `memory.New()` in a test, and in production the NATS JetStream
provider, below. `Shutdown` is the process's drain timeout, within which every reactor the runtime
builds drains. `Build` panics on a nil `Broker` or `Logger`, a wiring mistake, and returns an
error for a `Config` that is not finalized, a `Shutdown` that is not positive, or an engine missing
a statement; it does no I/O. Error handling is elided throughout.

A domain emits through `rt.Recorder`, in its command's own transaction, so an event is written
exactly when the state it reports commits:

```go
_, err := sqlate.Transact(ctx, db, rt.Recorder.Emit(ctx, func(tx *sqlate.Tx, q *event.Queue) (struct{}, error) {
	// ... the command's writes on tx ...
	InvoiceIssued.Raise(q, invoiceID, InvoiceIssuedData{Amount: amount})
	return struct{}{}, nil
}), nil)
```

## The NATS broker

The `nats` sub-module's `Broker` publishes to one JetStream stream and subscribes durable pull
consumers on it. Its `Config` is the service's `nats` block, finalized as the messaging block is:

```go
owner := true // the one service that owns the stream; every other leaves Owner nil
ncfg := nats.Config{Stream: "billing", Prefix: "billing", Owner: &owner}
err := ncfg.Finalize("BILLING") // defaults url, reads BILLING_NATS_*, and validates

broker, err := nats.New(ncfg) // no I/O: Start connects, at the lowest stage
```

The broker publishes each event with its type as the subject, unchanged, so `prefix` is the
namespace of the service's event types: with the prefix `billing`, it publishes
`billing.invoice.issued`, and a consumer subscribes to that type. `Publish` refuses a type outside
the prefix as permanent, so the relay quarantines its row, and `Subscribe` refuses a subscription
naming one.

One service owns a stream: it sets `owner` true, and its `Start` creates the stream or updates it
to its config, with the subjects `<prefix>.>`, the deduplication window `duplicates`, and the
retention `max_age`. Every other service on the stream leaves `owner` unset or false: its broker
binds the stream as it finds it and writes nothing, and its `Start` fails when the stream is
missing or does not capture its prefix. Only the owner sets `duplicates` and `max_age`; a
non-owner that sets either fails validation, naming the field. In JSON:

```json
{"nats": {"url": "nats://nats:4222", "stream": "billing", "prefix": "billing", "owner": true, "max_age": "168h"}}
```

```json
{"nats": {"url": "nats://nats:4222", "stream": "billing", "prefix": "billing"}}
```

The broker is a lifecycle node of the graph, started before every reactor on it; a call that
reaches it before `Start` fails with `nats.ErrNotStarted`. `broker.Conn()` is the native
connection, for request and reply in the composition root.

## The relay and consumers as graph nodes

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
		}, nil)
		return err
	})
})
```

The handler's return is the delivery's outcome: nil acknowledges it, an error redelivers it after
`RetryDelay`, and an error wrapped with `event.Permanent` terminates it. Data that does not decode
is refused permanently.

## The error hook

The failures the relay and the consumers survive, which no caller sees as an error, go to the
runtime's hook, each as a `messaging.Failure` whose `Kind` names what failed: `relay` (a pass
ended by a transient publish failure or a database error), `quarantine`, `source` (an error a
consumer's source survived), or `exhausted`. With a nil `Setup.OnFailure`, the runtime logs each
failure at warn as `messaging failure`. The default logs every failure it is given: during an
outage, about four a second from the relay at the default poll, and from each consumer member on
the NATS broker one failed pull about every 1.1 seconds, so a service that wants failures
throttled, counted, or alerted on sets its own `OnFailure`:

```go
rt, err := messaging.Setup{
	Config: cfg, Broker: broker, Outbox: postgres.Outbox(), Inbox: postgres.Inbox(),
	Shutdown: shutdown, Logger: logger,
	OnFailure: func(ctx context.Context, f messaging.Failure) {
		failures.WithLabelValues(string(f.Kind)).Inc()
	},
}.Build()
```

A hook must not block for long: the relay and the consumer wait on it.

## Quarantine

An outbox row that can never be published, one whose event does not decode or whose publish
failed with an `event.Permanent` error, is quarantined in its own transaction, so it does not
hold the outbox back. The relay reports it as a `quarantine` failure carrying an
`*outbox.QuarantineError`. The row stays in `messaging_outbox` with `quarantined_at` and
`quarantine_reason` set; the relay never claims it again, `postgres.Pending` does not count it,
and the outbox's retention never deletes it.

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

## Retention

The relay never deletes a published row and the inbox never deletes a handled one, so both
tables grow until the service reclaims them. `postgres.OutboxRetention` and
`postgres.InboxRetention` each return a `*postgres.Retention` whose `Pass(ctx, budget)` deletes
at most `budget` rows older than its age, oldest first, and reports `more` when it deleted its
whole budget. The service supplies both ages; the module has no default:

```go
outboxes, err := postgres.OutboxRetention(db, 7*24*time.Hour) // published rows only
inboxes, err := postgres.InboxRetention(db, 7*24*time.Hour)   // must outlast every redelivery

more, err := outboxes.Pass(ctx, 500)
```

An inbox row's deletion lets a redelivery of its event be claimed again, so the inbox's age must
outlast every redelivery the broker can make. A pass runs on a pool and needs no transaction,
and `Pass` has the shape a sweeper's reclaimer takes, so a sweeper that bounds each pass by a
budget drives it. Migrations 0002 and 0003 index both tables for their retentions.

## MaxDeliver exhaustion

A subscription's `MaxDeliver` bounds the deliveries of one event; 0 is unlimited. A handler reads
which delivery it is on with `messaging.Attempt(ctx)`. When the handler fails, with an error not
marked permanent, on the delivery whose attempt is `MaxDeliver`, the broker gives the event up and
the runtime reports an `exhausted` failure with the event's identity and the attempt.

Exhaustion is seen only from the handler's error. An event whose last delivery overruns
`AckWait`, with the handler returning nil or returning late, is given up unreported. Size
`AckWait` above the handler's worst case where an unreported loss matters.

## The quiesce gate

`Setup.Gate` gives the runtime a gate its reactors hold shared, such as go-core's
`reactor.Gate`, so an operation that holds it exclusively, a restore or a migration, runs with
the messaging paused:

```go
var gate reactor.Gate
rt, err := messaging.Setup{
	Config: cfg, Broker: broker, Outbox: postgres.Outbox(), Inbox: postgres.Inbox(),
	Shutdown: shutdown, Logger: logger,
	Gate: &gate,
}.Build()
```

The relay holds the gate for each outbox row, from before the row's transaction begins until it
commits or rolls back, so while the gate is held exclusively it opens no transaction. Each
delivery a `Consume` reactor handles holds it for the handler's whole call. A wait the drain ends
reports nothing: the relay claims no row, and the consumer hands its handler nothing and leaves
the event for redelivery. A delivery's wait counts against its `AckWait`, so one that waits past
it is redelivered and counts against `MaxDeliver`. With a nil `Gate`, nothing holds a gate; a
non-nil interface holding a nil pointer is a set gate, and is held.
