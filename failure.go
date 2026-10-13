package messaging

import (
	"context"
	"errors"
	"log/slog"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-messaging/outbox"
)

// FailureKind names what failed in a [Failure].
type FailureKind string

const (
	// FailureRelay is a relay pass that ended on an error: a transient
	// publish failure, after which the next pass retries the event, or a
	// database error.
	FailureRelay FailureKind = "relay"
	// FailureQuarantine is an outbox row the relay quarantined, committed
	// by the time it is reported: the row does not decode, or its publish
	// failed with an error marked [event.Permanent]. Its event is never
	// published.
	FailureQuarantine FailureKind = "quarantine"
	// FailureSource is an error a consumer's source survived, such as a
	// message it could not decode: the [Report] it was subscribed with.
	FailureSource FailureKind = "source"
	// FailureExhausted is an event whose handler failed, with an error not
	// marked [event.Permanent], on the subscription's last delivery, its
	// MaxDeliver: the broker gives the event up, and nothing redelivers it.
	FailureExhausted FailureKind = "exhausted"
)

// Failure is a messaging failure the runtime reports to its [Hook]: one
// that no caller sees as an error, because the relay or the consumer
// survives it.
type Failure struct {
	// Kind is what failed.
	Kind FailureKind
	// Consumer is the subscription's Name, for a source or exhausted
	// failure; it is empty for the relay's.
	Consumer string
	// EventSource, EventID, and EventType identify the event, when it is
	// known: always for an exhausted failure, and for a relay or
	// quarantine failure whose row decoded. They are empty otherwise.
	EventSource string
	EventID     string
	EventType   string
	// Attempt is the delivery the handler failed on, for an exhausted
	// failure; it is 0 for the others, which no delivery attempt names.
	Attempt int
	// Err is the failure itself: the handler's error, the error the source
	// reported, or the relay's report, an [*outbox.QuarantineError], an
	// [*outbox.PublishError], or a database error.
	Err error
}

// identify fills f's event identity from e.
func (f Failure) identify(e event.Event) Failure {
	f.EventSource, f.EventID, f.EventType = e.Source, e.ID, e.Type
	return f
}

// Hook receives each [Failure] the runtime reports, with the context it
// was reported under. It must not block for long: the relay and the
// consumer wait on it before they go on.
type Hook func(ctx context.Context, f Failure)

// Option configures a [Runtime].
type Option func(*Runtime)

// OnFailure sets the hook the runtime reports each [Failure] to. Without
// it, or with a nil hook, the runtime logs each failure at warn on its
// logger. The default logs every failure it is given, so during an outage
// it logs each failed relay pass, about four a second at the default poll;
// a service that wants failures throttled, counted, or alerted on sets a
// hook that does so.
func OnFailure(h Hook) Option {
	return func(r *Runtime) {
		if h != nil {
			r.hook = h
		}
	}
}

// logFailures returns the default hook: each failure logged at warn on
// logger as "messaging failure", with its kind, the consumer and the
// event's identity when known, the attempt when one is, and the error.
func logFailures(logger *slog.Logger) Hook {
	return func(ctx context.Context, f Failure) {
		attrs := []any{"kind", string(f.Kind)}
		if f.Consumer != "" {
			attrs = append(attrs, "consumer", f.Consumer)
		}
		if f.EventID != "" {
			attrs = append(attrs, "type", f.EventType, "event", f.EventID, "source", f.EventSource)
		}
		if f.Attempt > 0 {
			attrs = append(attrs, "attempt", f.Attempt)
		}
		logger.WarnContext(ctx, "messaging failure", append(attrs, "error", f.Err)...)
	}
}

// relayFailure classifies err, an error the relay reported: a quarantine
// as [FailureQuarantine], and a publish failure or a database error as
// [FailureRelay], with the event's identity when the relay knew it.
func relayFailure(err error) Failure {
	if q, ok := errors.AsType[*outbox.QuarantineError](err); ok {
		return Failure{Kind: FailureQuarantine, Err: err}.identify(q.Event)
	}
	if p, ok := errors.AsType[*outbox.PublishError](err); ok {
		return Failure{Kind: FailureRelay, Err: err}.identify(p.Event)
	}
	return Failure{Kind: FailureRelay, Err: err}
}
