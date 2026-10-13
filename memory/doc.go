// Package memory is the in-memory messaging provider: the conformance double
// for the standard tier and a test double for a service.
//
// The package exports:
//
//   - [Broker], a [messaging.Broker] held in memory, with [New], which
//     returns an empty one; [Broker.Publish], which appends an event to its
//     log; and [Broker.Subscribe], which returns a source on a durable
//     consumer
//
// A [Broker] keeps one append-only log of encoded events, as a JetStream
// stream would, and a durable consumer for each subscription Name that
// starts at the log's beginning, or at its end under [messaging.StartNew],
// once Subscribe creates it. Publish encodes an event to binary content
// mode and each delivery decodes it, so the codec a real binding uses is
// exercised on every message; one that fails to decode is passed to the
// subscription's report and terminated. Each delivery's context carries its
// attempt, the event's delivery count. Like a stream with a deduplication
// window, the broker drops a publish of a source and id it has seen within
// [messaging.DefaultDuplicates]. Nothing persists beyond the process.
//
// The package reads in three layers: broker.go holds the log and the
// consumers, consumer.go is each consumer's delivery state machine, and
// source.go is the member loop a reactor runs.
package memory
