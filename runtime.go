package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-core/reactor"
	"github.com/standards-lab/go-messaging/inbox"
	"github.com/standards-lab/go-messaging/outbox"
	"github.com/standards-lab/sqlate"
)

// Runtime is one service's messaging, built by [Setup.Build] from its
// [Config], the broker its provider supplies, and its database engine's
// outbox and inbox statements. The broker carries events to and from the
// service. The outbox holds the events a command emits until the relay
// publishes them, and the inbox records the events a consumer has handled.
// A domain takes Recorder, its one value for emitting events, and records
// each event in its command's transaction.
//
// The failures the relay and the consumers survive, which no caller sees
// as an error, go to the runtime's [Hook], set as [Setup.OnFailure]: each
// as a [Failure] naming what failed.
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
	gate     Gate
}

// Setup is what [Setup.Build] builds a service's [Runtime] from. Config,
// Broker, Outbox, Inbox, Shutdown, and Logger are required; OnFailure and
// Gate are optional, and nil selects their default.
type Setup struct {
	// Config is the service's messaging block, which must be finalized.
	Config Config
	// Broker carries the service's events to and from it; it must not be
	// nil.
	Broker Broker
	// Outbox is the database engine's outbox statements, each of which must
	// be set.
	Outbox outbox.Engine
	// Inbox is the database engine's inbox statements, each of which must be
	// set.
	Inbox inbox.Engine
	// Shutdown is the process's drain timeout, which every reactor the
	// runtime builds drains within; it must be positive.
	Shutdown time.Duration
	// Logger is what the runtime logs to; it must not be nil.
	Logger *slog.Logger
	// OnFailure is the hook the runtime reports each [Failure] to. When it
	// is nil, the runtime logs each failure at warn on Logger, as "messaging
	// failure". The default logs every failure it is given, so during an
	// outage it logs each failed relay pass, about four a second at the
	// default poll, and each failed pull of the nats provider's sources, one
	// per consumer member about every 1.1 seconds, a pull's one-second bound
	// plus its 100ms wait before the next; a service that wants failures
	// throttled, counted, or alerted on sets a hook that does so.
	OnFailure Hook
	// Gate is the gate the runtime's reactors hold shared. When it is nil,
	// they hold none; a non-nil interface holding a nil pointer is a set
	// gate, and is held.
	//
	// The relay holds the gate for each outbox row, from before the row's
	// transaction begins until the transaction commits or rolls back, so a
	// relay waiting on it opens no transaction and claims no row. Its wait
	// ends with the relay's stop, and its last pass's with that pass's
	// bound, either of which claims nothing and reports nothing. Any other
	// failure of the gate ends the pass and is reported as a
	// [FailureRelay], leaving the relay ready, since it is not the
	// database's.
	//
	// Each delivery a [Runtime.Consume] reactor handles holds the gate for
	// the handler's whole call, and the handler is not called until it
	// does. A wait that the drain ends hands the handler nothing and leaves
	// the event unacknowledged, for the broker to redeliver. The wait is
	// bounded by the delivery's AckWait like the handler is: a delivery
	// that waits on the gate past AckWait is redelivered, and counts
	// against MaxDeliver, so an event whose last delivery waits that long is
	// given up unreported. Any other failure of the gate is the delivery's
	// outcome, as a transient error of the handler's would be: the event is
	// redelivered, and on its last delivery reported as a
	// [FailureExhausted].
	Gate Gate
}

// Build builds the service's messaging that s describes. It panics, before
// anything else, when Broker or Logger is nil, a wiring mistake in the
// caller. It returns an error for a Config that does not validate, a
// Shutdown that is not positive, or an Outbox or Inbox engine missing a
// statement. It does no I/O.
func (s Setup) Build() (*Runtime, error) {
	if s.Broker == nil {
		panic("messaging: Setup.Broker is nil")
	}
	if s.Logger == nil {
		panic("messaging: Setup.Logger is nil")
	}
	if err := s.Config.Validate(); err != nil {
		return nil, fmt.Errorf("messaging: config: %w", err)
	}
	if s.Shutdown <= 0 {
		return nil, fmt.Errorf("messaging: shutdown must be positive, got %v", s.Shutdown)
	}
	ob, err := outbox.New(s.Outbox)
	if err != nil {
		return nil, fmt.Errorf("messaging: %w", err)
	}
	in, err := inbox.New(s.Inbox)
	if err != nil {
		return nil, fmt.Errorf("messaging: %w", err)
	}
	hook := s.OnFailure
	if hook == nil {
		hook = logFailures(s.Logger)
	}
	return &Runtime{
		Recorder: event.NewRecorder(ob.Sink(), s.Config.Source, event.Options{}),
		broker:   s.Broker,
		outbox:   ob,
		inbox:    in,
		cfg:      s.Config,
		shutdown: s.Shutdown,
		logger:   s.Logger,
		hook:     hook,
		gate:     s.Gate,
	}, nil
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
//
// Under a [Setup.Gate], the relay holds the gate shared for each row, from
// before the row's transaction begins until it settles.
func (r *Runtime) Relay(db sqlate.Beginner) *reactor.Reactor[event.Event] {
	src := r.outbox.Relay(db, r.cfg.RelayPoll.Duration(), outbox.RelayOptions{
		Report:  func(ctx context.Context, err error) { r.hook(ctx, relayFailure(err)) },
		Timeout: r.cfg.RelayTimeout.Duration(),
		Drain:   r.shutdown / 4,
		// A nil runtime gate converts to a nil outbox.Gate, so the relay
		// holds none.
		Gate: r.gate,
	})
	publish := func(ctx context.Context, e event.Event) error {
		if err := r.broker.Publish(ctx, e); err != nil {
			return err
		}
		r.logger.InfoContext(ctx, "event published", "type", e.Type, "event", e.ID, "subject", e.Subject)
		return nil
	}
	return reactor.New(src, publish, reactor.Options{CancelAfter: reactor.CancelWithin(r.shutdown)})
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
//
// Under a [Setup.Gate], each delivery holds the gate shared for fn's whole
// call, and fn is not called until it does. A wait that the drain or the
// delivery's deadline ends hands fn nothing, logs and reports nothing, and
// leaves the event to be redelivered.
func (r *Runtime) Consume[T any](sub Subscription, fn func(ctx context.Context, data T, claim Claim) error) (*reactor.Reactor[event.Event], error) {
	report := func(ctx context.Context, err error) {
		r.hook(ctx, Failure{Kind: FailureSource, Consumer: sub.Name, Err: err})
	}
	src, err := r.broker.Subscribe(sub, report)
	if err != nil {
		return nil, err
	}
	handle := func(ctx context.Context, e event.Event) error {
		release, ended, err := r.share(ctx)
		if ended {
			// Nothing was handed over, and the error leaves the event to
			// the broker's redelivery.
			return err
		}
		var data T
		repeat := false
		// A gate that failed otherwise skips fn, and its error is the
		// delivery's outcome, as a transient error of fn's would be.
		if err == nil {
			defer release()
			if derr := json.Unmarshal(e.Data, &data); derr != nil {
				err = event.Permanent(fmt.Errorf("decode: %w", derr))
			} else {
				claim := func(ctx context.Context, tx *sqlate.Tx) (bool, error) {
					first, err := r.inbox.Claim(ctx, tx, sub.Name, e)
					repeat = err == nil && !first
					return first, err
				}
				err = fn(ctx, data, claim)
			}
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
	return reactor.New(src, handle, reactor.Options{CancelAfter: reactor.CancelWithin(r.shutdown)}), nil
}

// share holds the runtime's gate shared for the delivery whose handler
// context is ctx, waiting until ctx ends or its reactor begins to drain;
// without a gate it holds nothing and returns at once. ended reports a
// wait that ctx or the drain ended, which holds nothing.
func (r *Runtime) share(ctx context.Context) (release func(), ended bool, err error) {
	if r.gate == nil {
		return func() {}, false, nil
	}
	wait, cancel := context.WithCancel(ctx)
	defer cancel()
	if drain := reactor.Draining(ctx); drain != nil {
		go func() {
			select {
			case <-drain:
				cancel()
			case <-wait.Done():
			}
		}()
	}
	release, err = r.gate.Shared(wait)
	if err != nil {
		return nil, wait.Err() != nil && errors.Is(err, wait.Err()), fmt.Errorf("messaging: gate: %w", err)
	}
	return release, false, nil
}
