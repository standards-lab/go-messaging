// Package messagingtest is the conformance suite every messaging provider
// passes.
//
// The package exports:
//
//   - [Run], which runs every case against brokers from a provider's
//     constructor, each case as a subtest
//   - [RunJournal], which runs the journal's cases against brokers that are
//     also a [messaging.Journal], each case as a subtest
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
package messagingtest
