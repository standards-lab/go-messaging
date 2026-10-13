// Package nats is go-messaging's NATS JetStream broker provider.
//
// A [Broker] publishes to one stream and subscribes durable pull consumers
// on it. It is a lifecycle component at the lowest stage. [New] checks its
// [Config] and does no I/O, so a composition root can build it cold;
// [Broker.Start] connects to Config.URL, reconnecting without limit once it
// is up. A call that reaches the broker before Start fails with
// [ErrNotStarted], which the coordinator's stage order prevents, and a
// second Start fails with [ErrStarted].
//
// Config is also a service's nats configuration block. Its JSON form,
// [Config.Merge], and [Config.Finalize] follow go-core's config conventions.
// One broker on a stream is its owner, which Config.Owner marks; only the
// owner sets the stream's Duplicates and MaxAge.
//
// [Broker.Conn] is the native handle, for uses beyond the standard tier such
// as request and reply. It belongs in the composition root.
package nats
