package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-core/reactor"
	"github.com/standards-lab/go-messaging/inbox"
	"github.com/standards-lab/go-messaging/outbox"
	"github.com/standards-lab/sqlate"
)

// Runtime is one service's messaging, built from its [Config], the broker
// its provider supplies, and its database engine's outbox and inbox
// statements. The broker carries events to and from the service. The outbox
// holds the events a command emits until the relay publishes them, and the
// inbox records the events a consumer has handled. A domain takes Recorder,
// its one value for emitting events, and records each event in its
// command's transaction.
//
// The failures the relay and the consumers survive, which no caller sees
// as an error, go to the runtime's [Hook], set with [OnFailure]: each as a
// [Failure] naming what failed.
//
// A Runtime registers nothing and knows nothing of the coordinator: the
// composition root makes the broker and each reactor the runtime builds a
// node of its graph, which the coordinator starts, drains, and watches.
type Runtime struct {
	Recorder *event.Recorder[*sqlate.Tx]

	broker   Broker
	outbox   *outbox.Outbox
	inbox    *inbox.Inbox
	cfg      Config
	shutdown time.Duration
	logger   *slog.Logger
	hook     Hook
}

// New builds a service's messaging from cfg, over broker and the engine's
// statements, logging to logger. shutdown is the process's drain timeout,
// which every reactor the runtime builds drains within. New checks cfg,
// which is finalized, and shutdown, which must be positive. It does no I/O.
func New(cfg Config, broker Broker, outboxEngine outbox.Engine, inboxEngine inbox.Engine, shutdown time.Duration, logger *slog.Logger, opts ...Option) (*Runtime, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("messaging: config: %w", err)
	}
	if shutdown <= 0 {
		return nil, fmt.Errorf("messaging: shutdown must be positive, got %v", shutdown)
	}
	ob, err := outbox.New(outboxEngine)
	if err != nil {
		return nil, fmt.Errorf("messaging: %w", err)
	}
	in, err := inbox.New(inboxEngine)
	if err != nil {
		return nil, fmt.Errorf("messaging: %w", err)
	}
	r := &Runtime{
		Recorder: event.NewRecorder(ob.Sink(), cfg.Source),
		broker:   broker,
		outbox:   ob,
		inbox:    in,
		cfg:      cfg,
		shutdown: shutdown,
		logger:   logger,
		hook:     logFailures(logger),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r, nil
}

// Relay returns the reactor that publishes the outbox's committed events on
// db to the broker, with the bound [reactor.CancelWithin] gives the drain
// timeout. Each publish is bounded by the configuration's RelayTimeout. The
// relay makes a last pass, bounded at a quarter of the drain timeout and so
// below the reactor's bound, that publishes what the producers committed
// while they drained. The root therefore makes every node that commits
// events use the relay's node, so they drain before it. The relay logs each
// event it publishes at info with its type, id, and subject, so the
// service's log shows the traffic it sends.
//
// The relay reports each row it quarantines to the hook as a
// [FailureQuarantine], and each pass a publish failure or a database error
// ends as a [FailureRelay], with the event's identity when the relay knew
// it.
func (r *Runtime) Relay(db sqlate.Beginner) *reactor.Reactor[event.Event] {
	report := func(ctx context.Context, err error) { r.hook(ctx, relayFailure(err)) }
	src := r.outbox.Relay(db, r.cfg.RelayPoll.Duration(), report,
		outbox.Timeout(r.cfg.RelayTimeout.Duration()), outbox.Drain(r.shutdown/4))
	publish := func(ctx context.Context, e event.Event) error {
		if err := r.broker.Publish(ctx, e); err != nil {
			return err
		}
		r.logger.InfoContext(ctx, "event published", "type", e.Type, "event", e.ID, "subject", e.Subject)
		return nil
	}
	return reactor.New(src, publish, reactor.CancelWithin(r.shutdown))
}

// Claim records, in a command's transaction, that the consumer is handling
// the event it was bound to, and reports whether this is the first time. A
// command runs it first and, on false, commits nothing more. It is an
// alias, so a domain's own claim type of the same shape accepts it.
type Claim = func(ctx context.Context, tx *sqlate.Tx) (first bool, err error)

// Consume returns the reactor that consumes sub's events, with the bound
// [reactor.CancelWithin] gives the drain timeout. The reactor decodes each
// event's data into T, the consumer's own type for the payload, and hands it
// to fn with a [Claim] over the inbox under sub.Name, bound to the event, so
// fn's domain never sees the event and a redelivery changes nothing. Data
// that does not decode is refused with [event.Permanent], because no
// redelivery can fix it.
//
// The reactor logs each delivery with the consumer, the event's type, id,
// and subject, and its outcome, so the service's log shows the traffic it
// receives. The outcome is "event consumed" at info when handled; repeat,
// when the claim found the event handled already; or retried, with the
// error, when the broker will redeliver it. A permanent refusal, from the
// decode or from fn, is "event refused" at warn with the error, because the
// broker terminates the delivery and nothing else reports it.
//
// The reactor reports to the hook each error the source survives, as a
// [FailureSource], and each event fn fails, with an error not marked
// permanent, on the delivery whose [Attempt] is sub.MaxDeliver, as a
// [FailureExhausted]: the broker gives that event up. An unlimited
// subscription, a MaxDeliver of 0, exhausts nothing. Exhaustion is seen
// only from fn's error, so an event whose last delivery overruns AckWait,
// with fn returning nil or late, is given up unreported.
func (r *Runtime) Consume[T any](sub Subscription, fn func(ctx context.Context, data T, claim Claim) error) (*reactor.Reactor[event.Event], error) {
	report := func(ctx context.Context, err error) {
		r.hook(ctx, Failure{Kind: FailureSource, Consumer: sub.Name, Err: err})
	}
	src, err := r.broker.Subscribe(sub, report)
	if err != nil {
		return nil, err
	}
	handle := func(ctx context.Context, e event.Event) error {
		var data T
		repeat := false
		err := json.Unmarshal(e.Data, &data)
		if err != nil {
			err = event.Permanent(fmt.Errorf("decode: %w", err))
		} else {
			claim := func(ctx context.Context, tx *sqlate.Tx) (bool, error) {
				first, err := r.inbox.Claim(ctx, tx, sub.Name, e)
				repeat = err == nil && !first
				return first, err
			}
			err = fn(ctx, data, claim)
		}
		attrs := []any{"consumer", sub.Name, "type", e.Type, "event", e.ID, "subject", e.Subject}
		switch {
		case event.IsPermanent(err):
			r.logger.WarnContext(ctx, "event refused", append(attrs, "outcome", "refused", "error", err)...)
		case err != nil:
			r.logger.InfoContext(ctx, "event consumed", append(attrs, "outcome", "retried", "error", err)...)
			// A provider that gives no attempt leaves Attempt 0, which only
			// an unlimited subscription's MaxDeliver would equal.
			if attempt := Attempt(ctx); sub.MaxDeliver > 0 && attempt == sub.MaxDeliver {
				r.hook(ctx, Failure{Kind: FailureExhausted, Consumer: sub.Name, Attempt: attempt, Err: err}.identify(e))
			}
		case repeat:
			r.logger.InfoContext(ctx, "event consumed", append(attrs, "outcome", "repeat")...)
		default:
			r.logger.InfoContext(ctx, "event consumed", append(attrs, "outcome", "handled")...)
		}
		return err
	}
	return reactor.New(src, handle, reactor.CancelWithin(r.shutdown)), nil
}
