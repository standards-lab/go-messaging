// Package nats is go-messaging's NATS JetStream broker and work-queue
// provider.
//
// The package exports:
//
//   - [Broker], the provider: a [messaging.Broker] on one stream, built by
//     [New], with [Broker.Start], [Broker.Ready], [Broker.Shutdown],
//     [Broker.Publish], [Broker.Subscribe], and [Broker.Conn]; and a
//     [messaging.Journal] on the same stream, with [Broker.Append], which
//     stores an event on a subject the caller names, guarded by the
//     subject's last stream sequence, and [Broker.Read], which reads a
//     subject back in order
//   - [Config], the service's nats block, with [Config.Merge],
//     [Config.Finalize], and [Config.Validate]; and [DefaultURL], the server
//     Finalize fills in
//   - [Queue], a [messaging.Queue] on a work-queue stream, built by
//     [NewQueue], with [Queue.Start], [Queue.Ready], [Queue.Shutdown],
//     [Queue.Enqueue], and [Queue.Subscribe]
//   - [QueueConfig], the service's queue block, with [QueueConfig.Merge],
//     [QueueConfig.Finalize], and [QueueConfig.Validate]
//   - [ErrNotStarted], a call before Start, and [ErrStarted], a second Start
//   - [AckMargin], how much longer a consumer waits for an outcome than the
//     handler's deadline
//
// # Lifecycle
//
// A [Broker] publishes to one stream and subscribes durable pull consumers
// on it. It is a lifecycle component at the lowest stage. [New] checks its
// [Config] and does no I/O, so a composition root can build it cold;
// [Broker.Start] connects to Config.URL, reconnecting without limit once it
// is up. A call that reaches the broker before Start fails with
// [ErrNotStarted], which the coordinator's stage order prevents, and a
// second Start fails with [ErrStarted].
//
// # Stream ownership
//
// Config is also a service's nats configuration block. Its JSON form,
// [Config.Merge], and [Config.Finalize] follow go-core's config conventions.
// One broker on a stream is its owner, which Config.Owner marks: its Start
// creates the stream or updates it to its config. Every other broker binds
// the stream as it finds it, writes nothing, and fails to start when the
// stream is missing or does not capture its prefix. Only the owner sets the
// stream's Duplicates and MaxAge.
//
// # Subjects
//
// An event's subject is its type, unchanged, so a type names its subject
// across every service on the stream. Config.Prefix is the namespace of
// those types: the stream captures Prefix.>, so [Broker.Publish] refuses a
// type that is not under Prefix as [event.Permanent], and [Broker.Subscribe]
// refuses a subscription naming one. A broker with the prefix billing
// publishes billing.invoice.issued, and a consumer subscribes to that type.
// [Broker.Append] stores an event on the subject its caller names instead,
// which must be under Prefix too; the event's type is unchanged, and a
// consumer's type filter matches the subject. [Broker.Read] reads such a
// subject, under Prefix as well.
//
// # Sources
//
// A source from [Broker.Subscribe] binds its durable consumer at its first
// Receive and pulls one message at a time. Each handler's context carries
// the delivery's attempt and a deadline of the subscription's AckWait. The
// source reports each error it survives to its report function, among them
// every failed pull. A pull that ends empty while the connection is down
// counts as failed, so during an outage a member reports about every 1.1
// seconds and is not ready until a pull succeeds again. Receive fails with
// [ErrNotStarted] before the broker's Start, and when the binding fails, as
// when the Name's consumer has another configuration; it ends with an error
// when the connection closes or the consumer or its stream is deleted. A
// message it cannot decode, or whose metadata does not parse, is terminated
// and reported. A handler error never ends Receive.
//
// # Work queues
//
// A [Queue] is a lifecycle component like a [Broker], on a stream of its
// own with work-queue retention: [NewQueue] checks its [QueueConfig] and
// does no I/O, and [Queue.Start] connects. The owner, which
// QueueConfig.Owner marks, creates the stream or updates it to its config,
// on file storage, capturing the work subject, the dead-letter subject, and
// every subject under the dead-letter subject; every other queue binds the
// stream as it finds it, and fails to start when the stream is missing, is
// not a work queue, or does not capture those subjects. [Queue.Enqueue]
// stores an event on the work subject with its source and id as
// Nats-Msg-Id, so a repeat within [messaging.DefaultDuplicates] is stored
// once.
//
// The workers subscribed under one Name share one durable pull consumer on
// their subject, which the source binds at its first Receive; each pulls one
// job at a time. A work-queue stream refuses a second consumer whose subject
// overlaps the first's, so one worker group takes each subject. While the
// handler runs, the source extends the delivery four times per AckWait, and
// when no extension is answered for AckWait it ends the handler's context,
// which carries the delivery's end for [messaging.Runtime.Work]. A nil
// acknowledges the job, and an error naks it with the attempt's
// [messaging.Worker.RetryDelay]. The handler's context carries the attempt,
// the server's delivered count. The consumer sets no delivery limit: the
// source enforces the worker's MaxDeliver, and dead-letters a delivery past
// it without calling the handler. A permanent error, an error on the final
// attempt, and a message that does not decode dead-letter the job too. A
// dead letter is the stored message, unchanged, published on the worker's
// DeadLetter with the dead-letter headers and a Nats-Msg-Id from that
// subject and the event's source and id, so it is stored once if
// dead-lettering repeats; only once the stream holds it is the job
// acknowledged, and a failed publish naks the job instead, so its next
// delivery, after the attempt's RetryDelay, dead-letters it again. A source
// on a dead-letter subject puts each dead letter's [messaging.DeadLetter] on
// the handler's context. A drained handler's nil settles its job, a
// permanent error dead-letters it, and any other error naks it at once.
//
// [Broker.Conn] is the native handle, for uses beyond the standard tier such
// as request and reply. It belongs in the composition root.
//
// The natstest package gives each test its own stream, for this module's
// integration suite and a service's own.
package nats
