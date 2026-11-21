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

A tool's module is an indirect requirement, so `scripts/currency.sh` scans it apart from the
direct requirements; go list's standard error, which carries the warning for a module with no tool
directive and any download's progress, is shown only when go list fails. An image tag counts only
when it has at least one dot and the pin's variant suffix (18.6-alpine -> -alpine): some images
also publish bare build numbers that would sort above every release. A compose service built from
`compose/<service>/Dockerfile` is pinned on its FROM line, not an image: line.
