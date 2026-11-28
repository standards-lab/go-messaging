package nats

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-core/reactor"
	"github.com/standards-lab/go-messaging"
)

// Queue is a [messaging.Queue] on a JetStream stream with work-queue
// retention.
type Queue struct {
	cfg      QueueConfig
	starting atomic.Bool
	conn     atomic.Pointer[conn]
}

var _ messaging.Queue = (*Queue)(nil)

// NewQueue checks cfg and returns a queue on it, unconnected. It does no
// I/O.
func NewQueue(cfg QueueConfig) (*Queue, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Queue{cfg: cfg}, nil
}

// Start connects to the server, waiting until ctx's deadline or 10s, and
// readies the stream. Once up, the connection reconnects without limit.
//
// The owner provisions the stream: it creates it, or updates it to the
// queue's configuration, with file storage, work-queue retention, the
// subjects Subject, DeadLetter, and DeadLetter.>, the deduplication window
// [messaging.DefaultDuplicates], and MaxAge. A non-owner writes nothing:
// it binds the stream as it finds it, and fails when the stream is missing,
// wrapping [jetstream.ErrStreamNotFound], when its retention is not
// work-queue, or when its subjects do not capture Subject, DeadLetter, and
// every subject under DeadLetter.
//
// A failed Start closes what it opened, and the queue can start again. A
// queue starts once: a Start while another is in progress or has succeeded
// fails with [ErrStarted] and opens nothing.
func (q *Queue) Start(ctx context.Context) (err error) {
	if !q.starting.CompareAndSwap(false, true) {
		return ErrStarted
	}
	defer func() {
		if err != nil {
			q.starting.Store(false)
		}
	}()
	ready := q.bind
	if q.cfg.owner() {
		ready = q.provision
	}
	c, err := connect(ctx, q.cfg.URL, q.cfg.Name, ready)
	if err != nil {
		return err
	}
	q.conn.Store(c)
	return nil
}

func (q *Queue) provision(ctx context.Context, js jetstream.JetStream) (jetstream.Stream, error) {
	s, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       q.cfg.Stream,
		Subjects:   []string{q.cfg.Subject, q.cfg.DeadLetter, q.cfg.DeadLetter + ".>"},
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.WorkQueuePolicy,
		Duplicates: messaging.DefaultDuplicates,
		MaxAge:     q.cfg.MaxAge.Duration(),
	})
	if err != nil {
		return nil, fmt.Errorf("nats: provision stream %s: %w", q.cfg.Stream, err)
	}
	return s, nil
}

func (q *Queue) bind(ctx context.Context, js jetstream.JetStream) (jetstream.Stream, error) {
	s, err := js.Stream(ctx, q.cfg.Stream)
	if err != nil {
		return nil, fmt.Errorf("nats: bind stream %s: %w", q.cfg.Stream, err)
	}
	info := s.CachedInfo().Config
	if info.Retention != jetstream.WorkQueuePolicy {
		return nil, fmt.Errorf("nats: bind stream %s: its retention is %s, not work-queue", q.cfg.Stream, info.Retention)
	}
	subjects := info.Subjects
	matchedBy := func(subject string) bool {
		return slices.ContainsFunc(subjects, func(filter string) bool { return matches(filter, subject) })
	}
	underDead := slices.ContainsFunc(subjects, func(filter string) bool { return captures(filter, q.cfg.DeadLetter) })
	if !matchedBy(q.cfg.Subject) || !matchedBy(q.cfg.DeadLetter) || !underDead {
		return nil, fmt.Errorf("nats: bind stream %s: its subjects %q do not capture %s, %s, and %s.>", q.cfg.Stream, subjects, q.cfg.Subject, q.cfg.DeadLetter, q.cfg.DeadLetter)
	}
	return s, nil
}

// See docs/internals.md
func matches(filter, subject string) bool {
	want := strings.Split(subject, ".")
	toks := strings.Split(filter, ".")
	for i, tok := range toks {
		switch {
		case tok == ">":
			return i < len(want)
		case i >= len(want):
			return false
		case tok != "*" && tok != want[i]:
			return false
		}
	}
	return len(toks) == len(want)
}

// Ready reports whether the queue is started and its connection is up.
func (q *Queue) Ready() bool {
	c := q.conn.Load()
	return c != nil && c.nc.IsConnected()
}

// Shutdown drains the connection: its subscriptions stop, pending publishes
// and acknowledgements flush, and it closes. It returns once the connection
// is closed, at once for a queue that never started. When ctx ends first,
// it closes the connection without finishing the drain and returns ctx's
// error.
func (q *Queue) Shutdown(ctx context.Context) error {
	return q.conn.Load().drain(ctx)
}

// Enqueue stores e on Subject in binary content mode, with its source and
// id as Nats-Msg-Id, and returns once the stream holds it, as
// [messaging.Queue] describes; a repeat within the deduplication window is
// accepted and dropped. Whether or not the queue is started, it rejects
// what [Broker.Publish] rejects of an event's encoding: what
// [messaging.Encode] rejects, and a header value holding CR or LF.
// Otherwise it fails with [ErrNotStarted] before Start.
func (q *Queue) Enqueue(ctx context.Context, e event.Event) error {
	h, body, err := messaging.Encode(e)
	if err == nil {
		err = carriable(h)
	}
	if err != nil {
		return fmt.Errorf("nats: enqueue: %w", err)
	}
	c := q.conn.Load()
	if c == nil {
		return ErrNotStarted
	}
	msg := &natsgo.Msg{Subject: q.cfg.Subject, Header: natsgo.Header(h), Data: body}
	msg.Header.Set(jetstream.MsgIDHeader, msgID(e))
	if _, err := c.js.PublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("nats: enqueue %s: %w", e.ID, err)
	}
	return nil
}

// Subscribe validates w, and that its Subject, when set, and its
// DeadLetter are the queue's DeadLetter or under it, and returns a source
// that binds the group's durable consumer at its first Receive, as
// [messaging.Queue] describes. The source passes report each error it
// survives; a nil report discards them.
func (q *Queue) Subscribe(w messaging.Worker, report messaging.Report) (reactor.Source[event.Event], error) {
	if err := w.Validate(); err != nil {
		return nil, err
	}
	if w.Subject != "" && !within(w.Subject, q.cfg.DeadLetter) {
		return nil, fmt.Errorf("nats: subscribe %s: subject %q is not the queue's dead-letter subject %q or under it", w.Name, w.Subject, q.cfg.DeadLetter)
	}
	if !within(w.DeadLetter, q.cfg.DeadLetter) {
		return nil, fmt.Errorf("nats: subscribe %s: dead letter %q is not the queue's dead-letter subject %q or under it", w.Name, w.DeadLetter, q.cfg.DeadLetter)
	}
	w = w.Normalize()
	subject := w.Subject
	if subject == "" {
		subject = q.cfg.Subject
	}
	cfg := jetstream.ConsumerConfig{
		Durable:       w.Name,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       w.AckWait + AckMargin,
		MaxDeliver:    w.MaxDeliver + 1,
		FilterSubject: subject,
	}
	return &worker{q: q, w: w, name: w.Name, stream: q.cfg.Stream, cfg: cfg, report: report}, nil
}
