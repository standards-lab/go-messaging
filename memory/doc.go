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
//   - [Queue], a [messaging.Queue] held in memory, with [NewQueue], which
//     returns an empty one; [Queue.Enqueue], which stores an event as a job;
//     and [Queue.Subscribe], which returns a source of a worker group's jobs
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
// repeated append whose expected sequence matches returns the original
// event's sequence. Nothing persists
// beyond the process.
//
// A [Queue] is apart from any [Broker]: it keeps its jobs in enqueue order,
// each on a subject, the queue's own for an enqueued event and a worker's
// dead-letter subject for a dead letter, and each job's delivery state
// with it, so every source on a subject takes from the same jobs. A job's
// delivery is extended for as long as its handler runs, the in-process
// form of an extension that always succeeds. A handler whose source drains
// settles its job by a nil or a permanent error; any other error leaves
// the job for redelivery at once, as an attempt with no outcome. Each
// handler's context carries its delivery's end, which
// [messaging.DeliveryEnd] reads. Enqueue
// and each dead-lettering store a source and id once on a subject within
// [messaging.DefaultDuplicates], so a dead letter never collides with its
// event's enqueue. Like the broker, the queue encodes each job and each
// delivery decodes it; one that fails to decode is reported and
// dead-letters.
//
// The package reads in five layers: broker.go holds the log and the
// consumers, consumer.go is each consumer's delivery state machine, and
// source.go is the member loop a reactor runs; queue.go holds the queue's
// jobs and their delivery state, and worker.go is the queue's member loop.
package memory
