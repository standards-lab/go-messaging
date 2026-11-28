package nats

import (
	"context"
	"errors"
	"fmt"
	"slices"
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
	pullWait            = time.Second
	settleWait          = 5 * time.Second
	retryWait           = 100 * time.Millisecond
	failuresBeforeCheck = 3
)

// Subscribe validates sub, and that each of its types is under Prefix, as
// every type the stream captures is, and returns a source that binds its
// durable consumer at its first Receive. The source passes report each
// error it survives; a nil report discards them.
func (b *Broker) Subscribe(sub messaging.Subscription, report messaging.Report) (reactor.Source[event.Event], error) {
	if err := sub.Validate(); err != nil {
		return nil, err
	}
	for _, t := range sub.Types {
		if err := b.namespaced("type", t); err != nil {
			return nil, fmt.Errorf("nats: subscribe %s: %w", sub.Name, err)
		}
	}
	sub = sub.Normalize()
	return &source{b: b, sub: sub, name: sub.Name, stream: b.cfg.Stream, cfg: b.consumerConfig(sub), report: report}, nil
}

func (b *Broker) consumerConfig(sub messaging.Subscription) jetstream.ConsumerConfig {
	maxDeliver := sub.MaxDeliver
	if maxDeliver == 0 {
		maxDeliver = -1
	}
	deliver := jetstream.DeliverAllPolicy
	if sub.Start == messaging.StartNew {
		deliver = jetstream.DeliverNewPolicy
	}
	cfg := jetstream.ConsumerConfig{
		Durable:        sub.Name,
		DeliverPolicy:  deliver,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        sub.AckWait + AckMargin,
		MaxDeliver:     maxDeliver,
		FilterSubjects: slices.Clone(sub.Types),
	}
	return cfg
}

// See docs/internals.md
type source struct {
	puller
	b   *Broker
	sub messaging.Subscription
}

func (s *source) Receive(ctx context.Context, fn reactor.Func[event.Event]) error {
	c := s.b.conn.Load()
	if c == nil {
		return ErrNotStarted
	}
	return s.receive(ctx, c, func(ctx context.Context, msg jetstream.Msg) { s.deliver(ctx, msg, fn) })
}

type puller struct {
	name   string
	stream string
	cfg    jetstream.ConsumerConfig
	report messaging.Report
	ready  atomic.Bool
}

func (p *puller) receive(ctx context.Context, c *conn, deliver func(context.Context, jetstream.Msg)) error {
	cons, err := c.js.CreateConsumer(ctx, p.stream, p.cfg)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("nats: bind consumer %s: %w", p.name, err)
	}
	p.ready.Store(true)
	defer p.ready.Store(false)
	failures := 0
	for ctx.Err() == nil {
		pctx, cancel := context.WithTimeout(ctx, pullWait)
		msg, err := cons.Next(jetstream.FetchContext(pctx))
		cancel()
		switch {
		case err == nil:
			failures = 0
			p.ready.Store(true)
			deliver(ctx, msg)
		case ctx.Err() != nil:
		case empty(err) && c.nc.IsConnected():
			failures = 0
			p.ready.Store(true)
		case errors.Is(err, natsgo.ErrConnectionClosed), errors.Is(err, jetstream.ErrConsumerDeleted):
			return fmt.Errorf("nats: receive %s: %w", p.name, err)
		case empty(err) && c.nc.IsClosed():
			return fmt.Errorf("nats: receive %s: %w", p.name, natsgo.ErrConnectionClosed)
		default:
			if empty(err) {
				err = fmt.Errorf("connection %s: %w", c.nc.Status(), err)
			}
			p.ready.Store(false)
			p.reportf(ctx, "pull: %w", err)
			failures++
			if c.nc.IsConnected() && (errors.Is(err, natsgo.ErrNoResponders) || failures >= failuresBeforeCheck) {
				if cause := gone(ctx, cons); cause != nil {
					return fmt.Errorf("nats: receive %s: %w", p.name, cause)
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

func empty(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, natsgo.ErrTimeout) || errors.Is(err, jetstream.ErrNoMessages)
}

func gone(ctx context.Context, cons jetstream.Consumer) error {
	ictx, cancel := context.WithTimeout(ctx, pullWait)
	defer cancel()
	_, err := cons.Info(ictx)
	if errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrStreamNotFound) {
		return err
	}
	return nil
}

func (s *source) deliver(ctx context.Context, msg jetstream.Msg, fn reactor.Func[event.Event]) {
	deadline := time.Now().Add(s.sub.AckWait)
	md, err := msg.Metadata()
	if err != nil {
		s.reportf(ctx, "message on %s: %w", msg.Subject(), err)
		s.settle(ctx, msg, "terminate", msg.Term)
		return
	}
	e, err := event.Decode(event.Header(msg.Headers()), msg.Data())
	if err != nil {
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

func (p *puller) settle(ctx context.Context, msg jetstream.Msg, what string, send func() error) {
	if err := send(); err != nil {
		p.reportf(ctx, "%s message on %s: %w", what, msg.Subject(), err)
	}
}

func (p *puller) reportf(ctx context.Context, format string, args ...any) {
	if p.report == nil {
		return
	}
	p.report(ctx, fmt.Errorf("nats: %s: "+format, append([]any{p.name}, args...)...))
}

func (p *puller) Ready() bool { return p.ready.Load() }
