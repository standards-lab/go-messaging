package nats

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-core/reactor"
	"github.com/standards-lab/go-messaging"
)

// AckMargin is how much longer the consumer waits for an outcome than the
// handler's deadline, so an outcome settled in time reaches the server
// before it redelivers, on a link whose round trip is well under the margin.
// On a slower link a settled event can be delivered again, never lost. The
// margin is part of the consumer's configuration, so a release that changes
// it cannot bind the durables an earlier one made.
const AckMargin = 250 * time.Millisecond

const (
	// pullWait bounds one pull, so a pull the server holds for a member that
	// has stopped expires soon.
	pullWait = time.Second
	// settleWait bounds the wait for the server to confirm an
	// acknowledgement.
	settleWait = 5 * time.Second
	// retryWait is how long a member waits after a pull fails before it
	// pulls again.
	retryWait = 100 * time.Millisecond
	// failuresBeforeCheck is how many pulls in a row may fail before the
	// member asks the server whether its consumer still exists.
	failuresBeforeCheck = 3
)

// Subscribe validates sub and returns a source that binds its durable
// consumer at its first Receive. The source passes report each error it
// survives; a nil report discards them.
func (b *Broker) Subscribe(sub messaging.Subscription, report messaging.Report) (reactor.Source[event.Event], error) {
	if err := sub.Validate(); err != nil {
		return nil, err
	}
	sub = sub.Normalize()
	return &source{b: b, sub: sub, cfg: b.consumerConfig(sub), report: report}, nil
}

// consumerConfig is the durable consumer the normalized sub describes. It
// is derived the same way for every member, so members of a Name bind one
// consumer, and a different subscription under the Name fails to bind.
func (b *Broker) consumerConfig(sub messaging.Subscription) jetstream.ConsumerConfig {
	maxDeliver := sub.MaxDeliver
	if maxDeliver == 0 {
		maxDeliver = -1 // JetStream's unlimited
	}
	deliver := jetstream.DeliverAllPolicy
	if sub.Start == messaging.StartNew {
		deliver = jetstream.DeliverNewPolicy
	}
	cfg := jetstream.ConsumerConfig{
		Durable:       sub.Name,
		DeliverPolicy: deliver,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       sub.AckWait + AckMargin,
		MaxDeliver:    maxDeliver,
	}
	for _, t := range sub.Types {
		cfg.FilterSubjects = append(cfg.FilterSubjects, b.subject(t))
	}
	return cfg
}

// source is one member of a durable consumer: the loop a reactor runs.
type source struct {
	b      *Broker
	sub    messaging.Subscription // normalized
	cfg    jetstream.ConsumerConfig
	report messaging.Report
	ready  atomic.Bool
}

// Receive binds the consumer, then delivers its messages to fn one at a
// time until ctx ends. It fails with [ErrNotStarted] before the broker's
// Start, and when the binding fails, as when the Name's consumer has
// another configuration. It ends with an error when the connection closes
// or the consumer or its stream is deleted. Every other error it survives
// goes to the report function: a message it cannot decode or whose
// metadata does not parse, both of which it terminates; an outcome that
// fails to send; and a failed pull, after which the source reports not
// ready until a pull succeeds again. A handler error never ends Receive.
func (s *source) Receive(ctx context.Context, fn reactor.Func[event.Event]) error {
	c := s.b.conn.Load()
	if c == nil {
		return ErrNotStarted
	}
	cons, err := c.js.CreateConsumer(ctx, s.b.cfg.Stream, s.cfg)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("nats: bind consumer %s: %w", s.sub.Name, err)
	}
	s.ready.Store(true)
	defer s.ready.Store(false)
	failures := 0
	for ctx.Err() == nil {
		// The pull's context ends with ctx, and its deadline sets the pull's
		// expiry on the server.
		pctx, cancel := context.WithTimeout(ctx, pullWait)
		msg, err := cons.Next(jetstream.FetchContext(pctx))
		cancel()
		switch {
		case err == nil:
			failures = 0
			s.ready.Store(true)
			s.deliver(ctx, msg, fn)
		case ctx.Err() != nil:
		case errors.Is(err, context.DeadlineExceeded), errors.Is(err, natsgo.ErrTimeout), errors.Is(err, jetstream.ErrNoMessages):
			// An empty pull is a healthy one.
			failures = 0
			s.ready.Store(true)
		case errors.Is(err, natsgo.ErrConnectionClosed), errors.Is(err, jetstream.ErrConsumerDeleted):
			return fmt.Errorf("nats: receive %s: %w", s.sub.Name, err)
		default:
			// Most failures pass, such as a reconnect or a leader election.
			// A deleted consumer or stream does not: its pulls fail with no
			// responders, so after a few failures the member asks the server.
			s.ready.Store(false)
			s.reportf(ctx, "pull: %w", err)
			failures++
			if errors.Is(err, natsgo.ErrNoResponders) || failures >= failuresBeforeCheck {
				if gone := s.gone(ctx, cons); gone != nil {
					return fmt.Errorf("nats: receive %s: %w", s.sub.Name, gone)
				}
			}
			select {
			case <-ctx.Done():
			case <-time.After(retryWait):
			}
		}
	}
	return nil
}

// gone returns the error that says cons or its stream no longer exists, or
// nil when it does or the server cannot say.
func (s *source) gone(ctx context.Context, cons jetstream.Consumer) error {
	ictx, cancel := context.WithTimeout(ctx, pullWait)
	defer cancel()
	_, err := cons.Info(ictx)
	if errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrStreamNotFound) {
		return err
	}
	return nil
}

// deliver handles one message and settles its outcome, unless the handler
// missed its deadline, receipt plus the subscription's AckWait, in which
// case the outcome is dropped and the server redelivers the message once
// the consumer's longer AckWait passes.
func (s *source) deliver(ctx context.Context, msg jetstream.Msg, fn reactor.Func[event.Event]) {
	deadline := time.Now().Add(s.sub.AckWait)
	// The attempt comes from the server's delivered count, so a message
	// whose metadata does not parse has none to give and is never handled.
	md, err := msg.Metadata()
	if err != nil {
		s.reportf(ctx, "message on %s: %w", msg.Subject(), err)
		s.settle(ctx, msg, "terminate", msg.Term)
		return
	}
	e, err := event.Decode(event.Header(msg.Headers()), msg.Data())
	if err != nil {
		// A message that cannot decode can never be handled, so it is
		// terminated, and reported, since no handler sees it.
		s.reportf(ctx, "message %d: %w", md.Sequence.Stream, err)
		s.settle(ctx, msg, "terminate", msg.Term)
		return
	}
	hctx, cancel := context.WithDeadline(messaging.WithAttempt(ctx, int(md.NumDelivered)), deadline)
	err = fn(hctx, e)
	cancel()
	if !time.Now().Before(deadline) {
		return
	}
	switch {
	case err == nil:
		s.settle(ctx, msg, "acknowledge", func() error {
			sctx, cancel := context.WithTimeout(context.Background(), settleWait)
			defer cancel()
			return msg.DoubleAck(sctx)
		})
	case event.IsPermanent(err):
		s.settle(ctx, msg, "terminate", msg.Term)
	case s.sub.RetryDelay > 0:
		s.settle(ctx, msg, "nak", func() error { return msg.NakWithDelay(s.sub.RetryDelay) })
	default:
		s.settle(ctx, msg, "nak", msg.Nak)
	}
}

// settle sends one outcome for msg, reporting a failure to send it.
func (s *source) settle(ctx context.Context, msg jetstream.Msg, what string, send func() error) {
	if err := send(); err != nil {
		s.reportf(ctx, "%s message on %s: %w", what, msg.Subject(), err)
	}
}

// reportf passes the report function an error the source survives, wrapped
// with the source's Name; with no report function it discards it.
func (s *source) reportf(ctx context.Context, format string, args ...any) {
	if s.report == nil {
		return
	}
	s.report(ctx, fmt.Errorf("nats: %s: "+format, append([]any{s.sub.Name}, args...)...))
}

// Ready reports whether the source is receiving.
func (s *source) Ready() bool { return s.ready.Load() }
