// Package nats is go-messaging's NATS JetStream broker provider.
//
// The package exports:
//
//   - [Broker], the provider: a [messaging.Broker] on one stream, built by
//     [New], with [Broker.Start], [Broker.Ready], [Broker.Shutdown],
//     [Broker.Publish], [Broker.Subscribe], and [Broker.Conn]
//   - [Config], the service's nats block, with [Config.Merge],
//     [Config.Finalize], and [Config.Validate]; and [DefaultURL], the server
//     Finalize fills in
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
// # Sources
//
// A source from [Broker.Subscribe] binds its durable consumer at its first
// Receive and pulls one message at a time. Each handler's context carries
// the delivery's attempt and a deadline of the subscription's AckWait. The
// source reports each error it survives to its report function, among them
// every failed pull. A pull that ends empty while the connection is down
// counts as failed, so during an outage a member reports about every 1.1
// seconds and is not ready until a pull succeeds again.
//
// [Broker.Conn] is the native handle, for uses beyond the standard tier such
// as request and reply. It belongs in the composition root.
//
// The natstest package gives each test its own stream, for this module's
// integration suite and a service's own.
package nats
