package nats

import (
	"context"
	"maps"
	"strconv"
	"strings"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-core/reactor"
	"github.com/standards-lab/go-messaging"
)

const extensionsPerAckWait = 4

var inProgress = []byte("+WPI")

// See docs/internals.md
type worker struct {
	puller
	q *Queue
	w messaging.Worker
}

func (s *worker) Receive(ctx context.Context, fn reactor.Func[event.Event]) error {
	c := s.q.conn.Load()
	if c == nil {
		return ErrNotStarted
	}
	return s.receive(ctx, c, func(ctx context.Context, msg jetstream.Msg) { s.deliver(ctx, c, msg, fn) })
}

func (s *worker) deliver(ctx context.Context, c *conn, msg jetstream.Msg, fn reactor.Func[event.Event]) {
	md, err := msg.Metadata()
	if err != nil {
		s.reportf(ctx, "message on %s: %w", msg.Subject(), err)
		s.settle(ctx, msg, "terminate", msg.Term)
		return
	}
	attempt := int(md.NumDelivered)
	e, err := event.Decode(event.Header(msg.Headers()), msg.Data())
	id := s.deadLetterID(md, e, err)
	switch {
	case attempt > s.w.MaxDeliver:
		s.deadLetter(ctx, c, msg, id, attempt, messaging.DeadLetter{Attempts: s.w.MaxDeliver})
		return
	case err != nil:
		s.reportf(ctx, "message %d: %w", md.Sequence.Stream, err)
		s.deadLetter(ctx, c, msg, id, attempt, messaging.DeadLetter{Attempts: attempt, Errored: true, Err: err.Error()})
		return
	}
	hctx, cancel := context.WithCancel(ctx)
	defer cancel()
	hctx = messaging.WithDeliveryEnd(messaging.WithAttempt(hctx, attempt), hctx.Done())
	if s.w.Subject != "" {
		if d, err := messaging.ReadDeadLetter(event.Header(msg.Headers())); err == nil {
			hctx = messaging.WithDeadLetter(hctx, d)
		}
	}
	extending, stop := context.WithCancel(context.Background())
	lost := make(chan bool, 1)
	go func() { lost <- s.extend(ctx, extending, c, msg, cancel) }()
	err = fn(hctx, e)
	stop()
	if <-lost {
		return
	}
	switch {
	case err == nil:
		s.ack(ctx, msg)
	case event.IsPermanent(err):
		s.deadLetter(ctx, c, msg, id, attempt, messaging.DeadLetter{Attempts: attempt, Errored: true, Err: err.Error()})
	case ctx.Err() != nil:
		s.settle(ctx, msg, "nak", msg.Nak)
	case attempt >= s.w.MaxDeliver:
		s.deadLetter(ctx, c, msg, id, attempt, messaging.DeadLetter{Attempts: attempt, Errored: true, Err: err.Error()})
	default:
		s.settle(ctx, msg, "nak", func() error { return msg.NakWithDelay(s.w.RetryDelay(attempt)) })
	}
}

func (s *worker) ack(ctx context.Context, msg jetstream.Msg) {
	s.settle(ctx, msg, "acknowledge", func() error {
		sctx, cancel := context.WithTimeout(context.Background(), settleWait)
		defer cancel()
		return msg.DoubleAck(sctx)
	})
}

// See docs/internals.md
func (s *worker) deadLetter(ctx context.Context, c *conn, msg jetstream.Msg, id string, attempt int, d messaging.DeadLetter) {
	h := natsgo.Header{}
	for name, vs := range msg.Headers() {
		if !strings.HasPrefix(strings.ToLower(name), "nats-") {
			h[name] = vs
		}
	}
	maps.Copy(h, natsgo.Header(d.Header()))
	h[jetstream.MsgIDHeader] = []string{id}
	pctx, cancel := context.WithTimeout(context.Background(), settleWait)
	_, err := c.js.PublishMsg(pctx, &natsgo.Msg{Subject: s.w.DeadLetter, Header: h, Data: msg.Data()})
	cancel()
	if err != nil {
		s.reportf(ctx, "dead-letter message on %s: %w", msg.Subject(), err)
		s.settle(ctx, msg, "nak", func() error { return msg.NakWithDelay(s.w.RetryDelay(attempt)) })
		return
	}
	s.ack(ctx, msg)
}

func (s *worker) deadLetterID(md *jetstream.MsgMetadata, e event.Event, decodeErr error) string {
	id := "d" + strconv.Itoa(len(s.w.DeadLetter)) + ":" + s.w.DeadLetter
	if decodeErr != nil {
		return id + "#" + strconv.FormatUint(md.Sequence.Stream, 10)
	}
	return id + msgID(e)
}

func (s *worker) extend(ctx, extending context.Context, c *conn, msg jetstream.Msg, cancel context.CancelFunc) (lost bool) {
	every := s.w.AckWait / extensionsPerAckWait
	deadline := time.Now().Add(s.w.AckWait)
	expiry := time.NewTimer(s.w.AckWait)
	defer expiry.Stop()
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-extending.Done():
			return false
		case <-expiry.C:
			cancel()
			return true
		case <-tick.C:
			until := time.Now().Add(every)
			if deadline.Before(until) {
				until = deadline
			}
			rctx, rcancel := context.WithDeadline(extending, until)
			_, err := c.nc.RequestWithContext(rctx, msg.Reply(), inProgress)
			rcancel()
			switch {
			case err == nil:
				deadline = time.Now().Add(s.w.AckWait)
				expiry.Reset(s.w.AckWait)
			case extending.Err() == nil:
				s.reportf(ctx, "extend message on %s: %w", msg.Subject(), err)
			}
		}
	}
}
