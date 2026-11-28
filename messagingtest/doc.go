// Package messagingtest is the conformance suite every messaging provider
// passes.
//
// The package exports:
//
//   - [Run], which runs every case against brokers from a provider's
//     constructor, each case as a subtest
//   - [RunJournal], which runs the journal's cases against brokers that are
//     also a [messaging.Journal], each case as a subtest
//   - [RunQueue], which runs the work queue's cases against queues from a
//     provider's constructor, each case as a subtest, and [QueueHarness],
//     the queue under test with the time it runs on, the dead-letter
//     subject the cases use, and the runtime their reactors are built from
//
// A provider's test calls [Run] with a constructor for a fresh broker; each
// case runs its sources through a [reactor.Reactor], as a composition root
// would.
//
// The cases prove what a service relies on: an event survives the broker
// intact, the type filter and delivery groups route it, a new Name starts
// where its subscription says, the handler's return decides its outcome,
// AckWait bounds a handler and redelivers the event, a durable keeps its
// position across members, and a drained handler's acknowledgement holds.
// Each delivery's context carries its attempt, which [messaging.Attempt]
// reads, counting up across redeliveries to MaxDeliver.
// The broker deduplicates on the event's source and id and rejects a type
// that breaks [messaging.CheckType]. A case that proves an absence, such as
// no redelivery after a terminate, watches a short quiet window.
//
// The journal's cases prove the guarded append: each append returns a
// stream sequence, and an expected sequence that is not the subject's last
// is [messaging.ErrConflict], unless the subject's last event is the one
// appended. An append shares the broker's deduplication window with
// Publish, and it rejects an event [messaging.Encode] rejects, a header
// value holding CR or LF, and a subject that breaks [messaging.CheckType].
// They prove the read: a subject's events come back after a sequence, in
// order and intact, ending where the subject stood when the read began, and
// a read stopped early holds nothing that blocks an append. An appended
// event reaches the broker's subscriptions, and a type filter matches its
// subject, not its type.
//
// The queue's cases run each worker's source directly, as its Receive,
// because a reactor detaches its handlers from the source's cancellation and
// the cases watch the handler's context end. They prove the job's life: an
// enqueued event waits for a worker, is stored once for its source and id,
// and goes to one worker of a group at a time; a delivery is extended past
// AckWait while its handler runs; a nil acknowledges, an error redelivers
// after its attempt's backoff, and a permanent error or a failed final
// attempt dead-letters the event, unchanged, with headers that say how its
// attempts ended. A final attempt left unsettled by a drained worker
// dead-letters on the next delivery without calling the handler. A
// dead-letter worker receives each dead letter with its
// [messaging.DeadLetter] and acknowledges it off. A drained handler's
// context ends; its nil holds and its error leaves the job for redelivery.
// One case runs its worker through a reactor that [messaging.Runtime.Work]
// builds, and proves that the reactor's drain ends the handler's context, so
// the reactor unwinds without cancelling it, and leaves the job for
// redelivery. Time passes through [QueueHarness.Advance], and a case that
// proves an absence enqueues a later event and waits for it, so the queue
// has acted on whatever was due before it.
package messagingtest
