// Package messaging is the standard tier's broker operations: publishing an
// event and subscribing to events as a reactor source.
//
// A [Broker] is what a composition root builds reactors from. A
// [Subscription] names a durable consumer: every source subscribed under
// one Name shares that consumer's position and splits its work, so replicas
// of a service form a delivery group, and each distinct Name receives every
// event. The handler's return is the delivery's outcome, so a reactor
// acknowledges, redelivers, or terminates without a broker type in sight,
// and [Attempt] reads which delivery of the event the handler is on. A
// source passes the errors it survives to the [Report] given to Subscribe.
//
// A [Runtime] is one service's messaging: the broker, the outbox and inbox
// over its database engine's statements, and the recorder its domains emit
// through. Its [Runtime.Relay] publishes the outbox, and its
// [Runtime.Consume] builds a consuming reactor that decodes each event into
// the consumer's own type and binds its inbox claim. The failures both
// survive reach the hook [OnFailure] sets, each as a [Failure], and are
// logged at warn without one. [Config] is the service's messaging
// configuration block, beside its provider's. The runtime registers
// nothing: the composition root makes the broker and each reactor a node of
// its graph, and the coordinator infers from their methods how to start,
// drain, and watch them.
//
// The package imports no broker and no engine: the composition root hands
// them to [New]. A provider, such as the
// memory package, implements [Broker], and the messagingtest package holds
// the conformance suite every provider passes. The postgres sub-module is
// the database engine for the outbox and inbox, and the nats sub-module is
// a broker provider.
package messaging
