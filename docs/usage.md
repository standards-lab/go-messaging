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

## Appending to a stream-backed root

A root whose state is its events, not a row, keeps them on one subject of the broker's stream and
reaches them through the broker's `messaging.Journal`. The service folds the subject's events into
the root's state, noting the last sequence it folded, and appends the event a command decides with
that sequence as the expected one:

```go
subject := "billing.account.42" // under the broker's prefix
var state Account
var last uint64 // 0: the subject has no event yet

for entry, err := range broker.Read(ctx, subject, last) {
	if err != nil {
		return err
	}
	state.Apply(entry.Event)
	last = entry.Sequence
}

seq, err := broker.Append(ctx, subject, last, e) // e decided from state, stamped
```

`Append` stores `e` only while `last` is the subject's last sequence, and returns its sequence.
When another writer appended first, it fails with an error wrapping `messaging.ErrConflict`; the
service reads the subject again after `last`, folds the new events into its state, decides
again, and appends with the new last sequence:

```go
for errors.Is(err, messaging.ErrConflict) {
	for entry, err := range broker.Read(ctx, subject, last) {
		// ... fold entry into state and set last, as above ...
	}
	// ... decide e again from state ...
	seq, err = broker.Append(ctx, subject, last, e)
}
```

After an error that leaves an append's outcome unknown, the service retries with the same event.
Its source and id identify it, so the retry returns the stored event's sequence rather than a
conflict, unless another event has been appended after it; then the retry conflicts, and the
service reads the subject again and finds its event among the new ones. A read ends at the
subject's last event as it stood when the read began, so an event appended during the read waits
for the next read.

An appended subject must sit under the broker's `Prefix`, which the stream captures; the NATS
broker refuses one outside it as `event.Permanent`. A root kept on a separate stream needs a
second broker, with that stream's `Config`. An appended event is a message on the stream like a
published one, so it reaches the stream's subscriptions. A subscription's type filter matches the
subject an event is on, not its type, so a consumer of appended events names their subjects in its
`Types`.

## The work queue

A `messaging.Queue` holds durable jobs apart from the broker's stream: each event enqueued waits
until one worker of a group acknowledges it, or until the queue dead-letters it. A service
enqueues a job with the event that describes it, stamped with its source, id, and type:

```go
err := queue.Enqueue(ctx, e) // returns once the queue holds e
```

`Enqueue` rejects what `messaging.Encode` rejects, as `Publish` does, and deduplicates the same
way: an event whose source and id the queue has seen within its window,
`messaging.DefaultDuplicates`, is accepted and stored once, so after an error that leaves an
enqueue's outcome unknown the service retries with the same event.

### Workers

A `messaging.Worker` is a durable group of workers: every source subscribed under its `Name`
shares the queue's jobs, and each member holds one job at a time.

```go
w := messaging.Worker{
	Name:       "render",           // the group; passes messaging.CheckName
	MaxDeliver: 5,                  // attempts on one job; attempt 5 is the final one
	AckWait:    time.Minute,        // a delivery's life with no successful extension; 0 is DefaultAckWait
	Backoff:    []time.Duration{time.Second, 10 * time.Second, time.Minute},
	DeadLetter: "jobs.billing.dead", // where the group's dead letters are stored
}
```

`Subject` is empty for the jobs enqueued, or a dead-letter subject for a dead-letter worker,
below. `Worker.Validate` reports every way a worker is unusable, together: a `Name` that fails
`messaging.CheckName`, a set `Subject` or a `DeadLetter` that fails `messaging.CheckType`, a
`MaxDeliver` that is not positive, a negative `AckWait` or `Backoff` entry, and a `DeadLetter`
that is missing or is the worker's own `Subject`. The queue's `Subscribe` validates the worker
and fails on an unusable one, so `Runtime.Work` does too. `Worker.RetryDelay(n)` is how long a
job waits after failed attempt `n`: `Backoff[n-1]`, the last entry past the end, and 0, a
redelivery at once, with no `Backoff`.

`rt.Work` builds the reactor that runs a worker group's jobs, the queue's counterpart of
`rt.Consume`. It decodes each job's data into the worker's own type and hands it to the handler;
data that does not decode is refused with `event.Permanent`, so the job dead-letters:

```go
render := g.Define("render", func(s *graph.Scope) (*reactor.Reactor[event.Event], error) {
	return s.Use(runtime).Work(s.Use(queue), w, func(ctx context.Context, job RenderJob) error {
		// ... the job's work, stopping when ctx ends ...
		return nil
	})
})
```

The reactor logs each delivery with the worker, the event's type, id, and subject, its attempt,
and its outcome: `job worked` at info when handled or retried, `job refused` at warn for a
permanent error, and `job exhausted` at warn for an error on the final attempt. It reports each
error its source survives to the hook as a `source` failure naming the worker. Under a
`Setup.Gate`, each delivery holds the gate for the handler's whole call, as a consumer's does;
the job's delivery is extended while it waits, and a wait the drain or the delivery's end ends
hands the handler nothing and leaves the job for redelivery. `Setup` still needs its `Broker`, its
outbox, and its inbox: the queue is given to `Work`, not to `Setup`.

### Extension and the handler's context

While the handler runs, the queue extends the job's delivery, so a long job keeps its claim; the
handler is given no way to extend it. The NATS queue sends an in-progress acknowledgement four
times per `AckWait`; the memory queue's extension always succeeds. When no extension has
succeeded for `AckWait`, as when the server is unreachable, the handler's context ends and the
job is left for redelivery to any member, so the handler stops when its context ends: its
outcome after that is ignored. The context also ends when the reactor drains. A drained
handler's nil still acknowledges its job and a permanent error still dead-letters it; any other
error leaves the job for redelivery at once.

A reactor detaches its handler from the source's cancellation, so the queue puts the delivery's
end on the handler's context and `Runtime.Work` ends the context from it: the handler need not
read `messaging.DeliveryEnd` itself.

### Outcomes

The handler's return is the job's outcome:

- nil acknowledges the job, which leaves the queue.
- An error redelivers it after `Worker.RetryDelay` of its attempt.
- An error wrapped with `event.Permanent` dead-letters it, on any attempt.
- Any error on the final attempt dead-letters it. `messaging.Attempt(ctx)` reads the attempt,
  from 1, and `w.Final(ctx)` reports whether it is the last, the attempt numbered `MaxDeliver`.
- A final attempt that ends with no outcome, because the worker drained with an error or lost its
  extension, dead-letters the job on its next delivery, without calling the handler.

A job leaves the queue only once its dead letter is stored. When the store fails, the job stays
and its next delivery, after its attempt's `RetryDelay`, dead-letters it again without calling
the handler, so a job past its final attempt always ends as a dead letter.

### Dead letters

A dead letter is the original event, unchanged, stored on the worker's `DeadLetter` subject with
three headers, which `messaging.ReadDeadLetter` reads back as a `messaging.DeadLetter`:

- `messaging-attempts` (`HeaderAttempts`): how many attempts were made, in decimal.
- `messaging-outcome` (`HeaderOutcome`): how the last attempt ended, `error` (`OutcomeError`) or
  `no-error` (`OutcomeNoError`) when it ended with no outcome.
- `messaging-last-error` (`HeaderLastError`): the last attempt's error, with CR and LF written as
  spaces, present only on `error`.

The names carry no `ce-` prefix, so the event decodes from the dead letter unchanged. A dead
letter is stored once however often dead-lettering repeats, and is never a duplicate of the
event's enqueue.

A dead-letter worker takes them off: its `Subject` is the dead-letter subject, and its own
`DeadLetter` another. Its handler's context carries each dead letter's `messaging.DeadLetter`,
which `messaging.DeadLetterOf` reads, and its nil acknowledges the dead letter off the queue:

```go
dead := messaging.Worker{
	Name:       "render-dead",
	Subject:    "jobs.billing.dead",
	MaxDeliver: 3,
	DeadLetter: "jobs.billing.dead.parked",
}
r, err := rt.Work(queue, dead, func(ctx context.Context, job RenderJob) error {
	d, _ := messaging.DeadLetterOf(ctx)
	// ... record or repair the job, from d.Attempts, d.Errored, and d.Err ...
	return nil
})
```

### Memory and NATS

`memory.NewQueue()` returns an empty in-memory queue, for a service's tests and the conformance
double. It has no lifecycle, its extension always succeeds, and nothing persists beyond the
process. Both providers pass `messagingtest.RunQueue`.

The `nats` sub-module's `Queue` keeps its jobs on a JetStream stream of its own with work-queue
retention, so an acknowledged job leaves the stream. Its `QueueConfig` is the service's queue
block, finalized as the broker's `Config` is, its environment overrides
`<PREFIX>_NATS_QUEUE_URL`, `…_NAME`, `…_STREAM`, `…_SUBJECT`, `…_DEAD_LETTER`, `…_OWNER`, and
`…_MAX_AGE`:

```go
owner := true // the one service that owns the queue's stream
qcfg := nats.QueueConfig{Stream: "billing_jobs", Subject: "jobs.billing.render", DeadLetter: "jobs.billing.dead", Owner: &owner}
err := qcfg.Finalize("BILLING") // defaults url, reads BILLING_NATS_QUEUE_*, and validates

queue, err := nats.NewQueue(qcfg) // no I/O: Start connects, at the lowest stage
```

`subject` is where `Enqueue` stores each job, and `dead_letter` the queue's dead-letter subject:
a worker's `DeadLetter`, and a dead-letter worker's `Subject`, must be it or under it, which
`Subscribe` checks. `subject` must not be `dead_letter` or under it. The stream's subjects must
not overlap another stream's on the server, so the queue's subjects sit outside the broker's
prefix.

As with the broker, one service owns the stream: its `Start` creates the stream or updates it
to its config, on file storage with work-queue retention, the subjects `subject`,
`dead_letter`, and `dead_letter.>`, the deduplication window `messaging.DefaultDuplicates`, and
the retention `max_age`. Every other service leaves `owner` unset or false: its queue binds the
stream as it finds it and writes nothing, and its `Start` fails when the stream is missing, is
not a work queue, or does not capture those subjects. Only the owner sets `max_age`, which
discards a job older than it whether or not a worker has taken it, and must be 0 or at least the
deduplication window. In JSON, under the key the service gives the block:

```json
{"queue": {"url": "nats://nats:4222", "stream": "billing_jobs", "subject": "jobs.billing.render", "dead_letter": "jobs.billing.dead", "owner": true, "max_age": "168h"}}
```

The queue is a lifecycle node of the graph, started before every reactor on it; a call that
reaches it before `Start` fails with `nats.ErrNotStarted`.

The workers under one `Name` share one durable pull consumer, and a work-queue stream refuses a
second consumer whose subject overlaps the first's: one worker group takes each subject. A
second group on the work subject fails at its first `Receive`, so a service that needs two kinds
of work enqueues them on two queues.
