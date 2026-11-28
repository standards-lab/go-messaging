# Internals

The reasoning behind go-messaging's unexported machinery, for a reader changing it. What each
package promises its consumers is in its `doc.go` and the [README](../README.md).

## memory: the consumer's state machine

A `consumer` is one durable consumer's delivery state. Every method runs with `Broker.mu` held and
is given the time rather than reading the clock. An event moves between three places:

```text
cursor ──take──▶ pending ──settle(nil or Permanent)──▶ done
                  │   ▲
settle(error) or  │   │ take, once due
expire (AckWait)  ▼   │
                 retries
```

An event that leaves pending by a failed settle or by expiry is terminated instead of retried once
MaxDeliver deliveries have been made; expired deliveries count toward MaxDeliver like failed ones.

- `take` claims a due retry first, then the next matching event past the cursor. With nothing to
  take, it returns the channel that signals new work and how long until a retry comes due or a
  delivery expires, so a waiting member wakes in time to act on it.
- Each delivery carries a token, which tells a current outcome from a late one. `settle` expires
  overrun deliveries before it applies an outcome, so the outcome of an expired delivery is
  ignored: the event has been, or will be, redelivered. Expiring first matters for a lone member,
  since no other member's `take` sweeps its overrun.
- `wakeAll` broadcasts by closing the wake channel and replacing it. A member captures the current
  channel under the lock, so it cannot miss a wake between looking for work and waiting.
- A member settles the event it is handling before it next checks its context, so a drained
  handler's outcome holds. A message that cannot decode can never be handled, so it is terminated
  and reported, since no handler sees it.

## memory: the queue's jobs

A `Queue` keeps its jobs in one slice, in enqueue order, under one mutex; every method on it runs
with the mutex held and is given the time, read from the queue's clock. A job is available,
claimed by one delivery, or gone:

```text
store ──take──▶ claimed ──settle(nil)──▶ gone
  ▲               │  ├──settle(Permanent, or error on the final attempt)──▶ dead letter, gone
  │ once due      │  └──settle(error)──▶ available after RetryDelay(attempt)
  └───────────────┘ expiry, or a drained handler's error: available at once
```

- `take` sweeps expired claims, then claims the earliest-due redelivery on the worker's subject,
  or else the first new job. A job whose attempts have reached the worker's MaxDeliver is not
  claimed: it dead-letters with the no-error outcome, since a failed final attempt has already
  dead-lettered, and `take` looks again.
- A claim is extended while its handler runs: the sweep moves an extending claim's deadline to
  AckWait past the time it is given, so no member's sweep can expire it. The test hook that loses
  extension fixes the deadline at AckWait past the loss; the member's watcher waits on the clock
  for it, and the sweep that expires the claim cancels the handler's context.
- `settle` sweeps first and ignores an outcome whose claim has gone, as the broker's consumer
  ignores a late token. The handler's context is a child of the member's, so a drain ends it; a
  nil or a permanent error from it still settles, and any other error makes the job available
  at once, an attempt with no outcome.
- A dead letter is stored like an enqueue, on the worker's dead-letter subject, its slot the
  subject with the event's source and id, so it is never a duplicate of its own enqueue and is
  stored once if dead-lettering repeats. The test hook that keeps the next dead-lettered job is
  how a repeat is made.
- The clock is the wall clock, or a test clock whose timers fire as it advances. A member
  registers its timer under the mutex, at an absolute time, so an advance cannot fall between
  reading the time and waiting.

## nats: the stream and its consumers

- **Capture.** A non-owner binds the stream and checks that its subjects capture every subject
  under Prefix, that is, every subject `Prefix.>` matches. Under NATS's wildcard rules `*` matches
  one token and a final `>` one or more. A stream subject captures when, token by token, each of
  its tokens before the prefix's end is `*` or the prefix's own token, and it ends in `>` no later
  than the token after the prefix: `Prefix.>` itself, a parent's `.>`, or `>`. A `*` in the token
  after the prefix captures only one more token, and a subject without `>` only a fixed number, so
  neither captures. For the same reason Publish refuses the prefix itself: it is outside
  `Prefix.>`.
- **Deduplication key.** A message's id is its event's source and id, which together identify a
  CloudEvents event. The source is length-prefixed, so no source and id pair spells another's key.
- **Append.** An append is a publish with the subject's expected last sequence and Publish's
  message id, so appends and publishes share the deduplication window. A single server checks the
  sequence before it deduplicates, so a retry whose first acknowledgement was lost conflicts; a
  cluster answers the same retry with a duplicate acknowledgement. To make the retry succeed on
  both, Append fetches the subject's last message on a conflict and returns its sequence when it
  decodes to the appended event's source and id. A replicated stream reports the conflict as
  error code 10164 rather than 10071, and Append treats both as a conflict. The memory broker
  follows a single server's order: it checks the sequence, then the deduplication window. The
  integration suite runs one server, so the cluster's order and code 10164 come from the server's
  source, untested here.
- **Read.** A read looks up the subject's last message once, as the iteration begins, and that
  message's sequence bounds the read, so an event appended during the read is not yielded. The
  read goes through an ordered consumer filtered on the subject, starting at the sequence after
  `after`, and stops at the bound. A delete or MaxAge can remove the bound's message before the
  consumer reaches it, so instead of waiting for a message that will not come, the read also
  stops at a message past the bound, without yielding it, and after a message behind which the
  consumer reports nothing pending. The bound stays the primary stop, because the pending count
  alone would yield an event appended during the read. An empty subject, or one whose last
  sequence is at or before `after`, creates no consumer. An ordered consumer outlives the
  client's subscription until the server's inactivity threshold, so the read deletes it however
  the read ends.
- **Binding.** The durable consumer is derived from the normalized subscription the same way for
  every member, its filter subjects the subscription's types, since each type is its subject. So
  the members of a Name bind one consumer, and a different subscription under the Name fails to
  bind. A MaxDeliver of 0 becomes JetStream's unlimited, -1, and the filter subjects are a clone,
  so the consumer's configuration shares no array with the subscription.
- **Pulls.** Each pull is bounded, so a pull the server holds for a member that has stopped
  expires soon, and its deadline sets the pull's expiry on the server. An empty pull is a healthy
  one while the connection is up. Most failures are transient, such as a reconnect or a leader
  election. While the connection is down the client buffers the pull request and nothing answers,
  so the pull ends as an empty one would; that is a failure too, named for the connection's state.
  A deleted consumer or stream is not transient: its pulls fail with no responders, so after a few
  failures the member asks the server whether its consumer still exists, which it can do only
  while connected.
- **Deliveries.** The attempt comes from the server's delivered count, so a message whose metadata
  does not parse has none to give and is terminated unhandled. A handler that misses its deadline,
  receipt plus the subscription's AckWait, has its outcome dropped, and the server redelivers the
  message once the consumer's longer AckWait passes.
- **Shutdown.** A connection that is reconnecting cannot drain, and its Drain call closes it, so
  Shutdown treats that error, like a closed connection's, as no failure and waits for the close.

## nats: the work queue

- **Stream.** The queue's stream has work-queue retention, so an acknowledged job leaves it. Its
  subjects are the work subject, the dead-letter subject, and every subject under the latter, so
  a dead-letter worker's own dead letters stay on the stream. A work-queue stream refuses two
  consumers whose filters overlap, so one worker group takes each subject. A non-owner checks
  that a stream subject matches each of the work and dead-letter subjects, token by token with
  NATS's wildcards, and that one captures every subject under the dead-letter subject, as the
  broker's capture check does for its prefix.
- **Pulls.** A worker pulls as the broker's source does, through the same loop, one message at
  a time, so each member holds one job.
- **Extension.** While the handler runs, the worker sends an in-progress acknowledgement four
  times per AckWait as a request, which the server answers, so the worker knows each extension
  arrived; an unanswered one is reported. Each request is bounded by the next tick and by the
  delivery's deadline, so the deadline passes on time while the server is silent. AckWait past
  the last answered extension, the handler's context ends and the worker leaves the job to the
  server, whose consumer AckWait adds AckMargin and so redelivers after the handler's context
  has ended. An acknowledgement that arrives after the server's AckWait but before any member
  pulls again still removes the job, so a late outcome is lost only to a redelivery already
  taken.
- **Outcomes.** The consumer's MaxDeliver is one past the worker's, so the delivery past the
  final attempt reaches the worker, which does not run the handler. A failed attempt naks with
  the attempt's RetryDelay; JetStream's consumer BackOff applies only to ack timeouts, so it is
  unset. A drained handler's error naks at once.

## outbox: the relay's transactions

- Under a gate, the gate is held before a row's transaction begins and released after it
  settles: its release is deferred first, so it runs after the rollback's. The wait is on the
  relay's context, so the stop or the drain ends it.
- `database/sql` rolls a transaction back when the context it began with ends, so the transaction
  begins on a context that outlives the stop: a row claimed before the stop is settled, and its
  handler keeps only its own deadline. Only the claim itself stops at the relay's context. The
  transaction's own deadline, twice the handler's, bounds a stalled database or a handler that
  ignores its deadline.
- Marking a row published and quarantining it must each change exactly one row: an engine whose
  statement changes none would have the relay claim the row again on every pass.
- A gated relay's failure to hold its gate, other than the wait's context ending, is reported but
  is not the database's failure, so it leaves the relay ready. The last pass's failure, on
  shutdown, is reported and leaves its row for the next start, as any failed pass does, so it is
  not Receive's error.

## postgres: statements and claims

The count of pending rows and the retention statements are outside the outbox's and inbox's
Engine contracts: the outbox never counts its rows, and neither the relay nor the inbox's claim
deletes one. The statements compile once for PostgreSQL; their files include no patterns, so the
catalog is sqlate's own, and a compile error is a defect in the embedded files that any test of the
package finds.

A consumer's inbox claim lives in its handler's transaction, which begins on the delivery's
context, so a handler that outlives AckWait holds its claim only until that context's deadline
rolls the transaction back. The redelivery, to another member of the group, then claims the event
and does the work once: its claim waits on the first handler's row lock rather than failing, and
under READ COMMITTED proceeds once the lock is released.

## messagingtest: the conformance cases' timing

Every wait for something that should happen is bounded by a failsafe, so a broken provider fails
the case instead of hanging it; a case watches a shorter quiet window for something that must not
happen. A provider may create a StartNew consumer as late as the source's first Receive, so the
StartNew case publishes its later events only once the member is ready. The drain case's AckWait
is short, so a lost acknowledgement would redeliver within the watch, which runs to twice AckWait
past the handler's start, leaving a provider room for its own margin.

The queue's cases take a `QueueHarness` whose `Advance` passes the queue's time: the memory
provider moves its test clock, and a real-time provider waits. Each AckWait and Backoff is a
multiple of the harness's `Unit`. A case that proves an absence advances past the moment the
unwanted delivery would come due, then enqueues a later event and waits for it: a queue delivers
a due redelivery before a new event, so the later event's arrival shows the queue has acted. A
JetStream consumer does: it serves its redelivery queue before the stream's new messages, and a
nak with a delay re-arms its redelivery timer within milliseconds. A
case advances only once the job it times has settled, shown by a later event's arrival, so the
backoff runs from a known time.

## Development configuration

### Lint configuration

The base module's `.golangci.yml` enables depguard alongside testpackage and nolintlint; the
postgres and nats sub-modules carry their own configuration, without depguard, and with the
integration build tag so the check lints each integration suite. A depguard entry matches by
prefix unless it ends in `$`, so each allow entry is anchored, as exactly a path or a path under
it. The longest match wins, so the deny entries keep the sub-modules out of the base despite the
module's own allow entry.

`postgres/sqlint.toml` runs from the postgres module's directory. The engine names the native
forms a standard-tier file may not use, and the sources name the pattern catalogs the statements'
includes resolve against: sqlate's own. The base module holds no SQL. The outbox's and the inbox's
statements are named for their operation, keep the delimiter out of comments and literals, and use
no native form in a standard-tier file; a native file is exempt by its tier and carries its port
note. The migrations have no native-forms check: the DDL is PostgreSQL's, the only engine's.

### The compose harness

`compose.yml` is test-only. Each service builds from `compose/<service>/Dockerfile`, which pins
its image on its FROM line, its one pin, and carries its configuration and health check. Each
keeps its data in tmpfs, so every start begins empty.

PostgreSQL is the server the integration suite connects to through `MESSAGING_DSN`, creating and
dropping a database per test. Its user, password, and database are the development values
`MESSAGING_DSN` names, not secrets. It is healthy once the server accepts TCP connections to the
app database: the first start's initialization runs a temporary server on the socket alone, which
would report ready before the real server starts.

NATS is the JetStream server the nats module's integration suite connects to through
`MESSAGING_NATS_URL`, creating and deleting a stream per test. It runs JetStream with its store in
`/data`, which compose mounts as tmpfs, and is healthy once JetStream is enabled, read from the
unpublished monitoring port.

### currency

A tool's module is an indirect requirement, so `scripts/currency.sh` scans it apart from the direct
requirements; go list's standard error, which carries the warning for a module with no tool
directive and any download's progress, is shown only when go list fails. Each scan captures its
command's output in a variable before reading it, so under `set -e` a scan command that fails (a go
list missing a go.sum entry) fails currency instead of reporting nothing. An image's latest tag
follows
[rolling currency](https://github.com/standards-lab/architecture/blob/main/principles/rolling-currency.md).
A compose service built from `compose/<service>/Dockerfile` is pinned on its FROM line, not an
image: line.
