// Package messagingtest is the conformance suite every messaging provider
// passes.
//
// The package exports:
//
//   - [Run], which runs every case against brokers from a provider's
//     constructor, each case as a subtest
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
package messagingtest
