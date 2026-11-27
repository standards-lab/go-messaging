// Package memory is the in-memory messaging provider: the conformance double
// for the standard tier and a test double for a service.
//
// The package exports:
//
//   - [Broker], a [messaging.Broker] and [messaging.Journal] held in
//     memory, with [New], which returns an empty one; [Broker.Publish],
//     which appends an event to its log on its type; [Broker.Subscribe],
//     which returns a source on a durable consumer; [Broker.Append], which
//     appends an event on a subject at an expected sequence; and
//     [Broker.Read], which reads a subject's events back
//
// A [Broker] keeps one append-only log of encoded events, as a JetStream
// stream would, numbered from 1 like a stream's sequences. Each event is on
// a subject, its type when published and the caller's subject when
// appended, and a consumer's type filter matches the subject. It keeps a
// durable consumer for each subscription Name that starts at the log's
// beginning, or at its end under [messaging.StartNew], once Subscribe
// creates it. Publish and Append encode an event to binary content mode and
// each delivery decodes it, so the codec a real binding uses is exercised on
// every message; one that fails to decode is passed to the subscription's
// report and terminated. Each delivery's context carries its attempt, the
// event's delivery count. Like a stream with a deduplication window, the
// broker stores a source and id once within [messaging.DefaultDuplicates],
// one window for Publish and Append: a repeated publish is dropped, and a
// repeated append returns the original event's sequence. Nothing persists
// beyond the process.
//
// The package reads in three layers: broker.go holds the log and the
// consumers, consumer.go is each consumer's delivery state machine, and
// source.go is the member loop a reactor runs.
package memory
