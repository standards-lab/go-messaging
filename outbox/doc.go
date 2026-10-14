// Package outbox writes events through a transactional outbox and publishes
// them with a relay, on any SQL engine that supplies the statements.
//
// The package exports:
//
//   - [Engine], every statement the outbox runs, compiled for one engine
//   - [Outbox], the outbox on one engine's statements, with [New], which
//     checks the engine and returns one; [Outbox.Sink], the event sink a
//     recorder writes through; and [Outbox.Relay], which returns its relay
//   - [Relay], the reactor source of the committed, unpublished events, with
//     [Relay.Receive], which runs its passes, and [Relay.Ready], its
//     readiness
//   - [RelayOption], which configures a Relay: [Timeout], the bound on one
//     row's handler; [Drain], the last pass after cancellation; and
//     [Gated], the [Gate] the relay holds shared for each row
//   - [QuarantineError], the relay's report of a row it quarantined, and
//     [PublishError], its report of a transient publish failure
//
// # Emitting
//
// An [Outbox] is built with [New] over an [Engine]: every statement the
// outbox runs, compiled for one engine by that engine's module, such as
// go-messaging's postgres sub-module. The engine's module also ships the
// tables as a migration set. This package holds no SQL and imports no
// driver.
//
// [Outbox.Sink] is the [event.Sink] a composition root builds a service's
// [event.Recorder] over. It inserts each event a command raises in the
// command's own transaction, so an event exists exactly when the state it
// reports was committed.
//
// # Relaying
//
// Nothing is published by the sink: the [Relay] publishes the committed
// rows afterward, each inside a transaction of its own that locks the row
// and marks it published once the broker holds the event, so a stop between
// the commit and the publish loses no event. A row that can never be
// published, one that does not decode or whose publish fails permanently,
// is quarantined in its own transaction instead, so it does not hold the
// outbox back, and the relay reports it as a [QuarantineError]. The inbox
// package guards a consumer against handling a redelivered event twice.
package outbox
