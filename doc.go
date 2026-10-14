// Package messaging is the standard tier's broker operations: publishing an
// event and subscribing to events as a reactor source.
//
// The package exports:
//
//   - [Publisher], which publishes an event, and [Broker], a Publisher that
//     also subscribes reactors; [Encode], the check and encoding every
//     provider's Publish makes; and [DefaultDuplicates], a broker's
//     deduplication window when it sets none
//   - [Subscription], a durable consumer, with [Subscription.Validate],
//     [Subscription.Normalize], the form a provider configures its consumer
//     from, and [Subscription.Matches], its type filter; [Start], where a new
//     consumer begins, [StartAll] or [StartNew]; and [DefaultAckWait]
//   - [CheckName], the rule for a consumer's or a stream's name, and
//     [CheckType], the rule for a routable event type
//   - [Report], which receives the errors a source survives
//   - [WithAttempt], which a provider calls to put a delivery's attempt on its
//     handler's context, and [Attempt], which reads it
//   - [Config], the service's messaging block, with [Config.Merge],
//     [Config.Finalize], and [Config.Validate]; [DefaultRelayPoll] and
//     [DefaultRelayTimeout]
//   - [Runtime], one service's messaging, with [New], which builds one;
//     [Runtime.Relay], the reactor that publishes the outbox; and
//     [Runtime.Consume], which builds a consuming reactor, handing its
//     handler a [Claim] over the inbox
//   - [Failure], a failure the runtime survives, and [FailureKind], what
//     failed: [FailureRelay], [FailureQuarantine], [FailureSource], or
//     [FailureExhausted]
//   - [Hook], which receives each Failure; [Option], which configures a
//     Runtime; [OnFailure], the Option that sets the hook; and [Gated], the
//     Option that sets the [Gate] the runtime's reactors hold shared
//
// # Brokers and subscriptions
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
// # The runtime
//
// A [Runtime] is one service's messaging: the broker, the outbox and inbox
// over its database engine's statements, and the recorder its domains emit
// through. Its [Runtime.Relay] publishes the outbox, and its
// [Runtime.Consume] builds a consuming reactor that decodes each event into
// the consumer's own type and binds its inbox claim. The failures both
// survive reach the hook [OnFailure] sets, each as a [Failure], and are
// logged at warn without one. Under [Gated], the relay holds a quiesce
// gate for each outbox row and a consumer for each delivery, so an
// operation that holds the gate exclusively runs with both paused.
// [Config] is the service's messaging configuration block, beside its
// provider's. The runtime registers nothing: the composition root makes the
// broker and each reactor a node of its graph, and the coordinator infers
// from their methods how to start, drain, and watch them.
//
// # Providers and engines
//
// The package imports no broker and no engine: the composition root hands
// them to [New]. A provider, such as the memory package, implements
// [Broker], and the messagingtest package holds the conformance suite every
// provider passes. The outbox and inbox packages run their tables on any
// engine that supplies the statements; the postgres sub-module is that
// engine for PostgreSQL, and the nats sub-module is the NATS JetStream
// provider.
//
// The base module depends on the standard library, go-core, and sqlate and
// its query package, the adjacent library the outbox's and inbox's engine
// contract builds on.
package messaging
