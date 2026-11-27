package nats

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-messaging"
)

const (
	connectWait = 10 * time.Second
	releaseWait = 10 * time.Second
)

// ErrNotStarted is the failure of a call that needs the connection, made
// before [Broker.Start].
var ErrNotStarted = errors.New("nats: broker not started")

// ErrStarted is the failure of a second [Broker.Start].
var ErrStarted = errors.New("nats: broker already started")

// Broker is a [messaging.Broker] on a JetStream stream.
type Broker struct {
	cfg      Config
	starting atomic.Bool
	conn     atomic.Pointer[conn]
}

type conn struct {
	nc     *natsgo.Conn
	js     jetstream.JetStream
	stream jetstream.Stream
}

var (
	_ messaging.Broker  = (*Broker)(nil)
	_ messaging.Journal = (*Broker)(nil)
)

// New checks cfg and returns a broker on it, unconnected. It does no I/O.
func New(cfg Config) (*Broker, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Broker{cfg: cfg}, nil
}

// Start connects to the server, waiting until ctx's deadline or 10s, and
// readies the stream. Once up, the connection reconnects without limit.
//
// The owner provisions the stream: it creates it, or updates it to the
// broker's configuration, with file storage, limits retention, the subjects
// Prefix.>, the deduplication window, and MaxAge. A non-owner writes
// nothing: it binds the stream as it finds it, and fails when the stream is
// missing, wrapping [jetstream.ErrStreamNotFound], or when none of its
// subjects captures every subject under Prefix, the namespace of every type
// the broker publishes.
//
// A failed Start closes what it opened, and the broker can start again. A
// broker starts once: a Start while another is in progress or has succeeded
// fails with [ErrStarted] and opens nothing.
func (b *Broker) Start(ctx context.Context) (err error) {
	if !b.starting.CompareAndSwap(false, true) {
		return ErrStarted
	}
	defer func() {
		if err != nil {
			b.starting.Store(false)
		}
	}()
	timeout := connectWait
	if d, ok := ctx.Deadline(); ok {
		timeout = time.Until(d)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("nats: connect %s: %w", b.cfg.URL, err)
	}
	nc, err := natsgo.Connect(b.cfg.URL, natsgo.Name(b.cfg.Name), natsgo.Timeout(timeout), natsgo.MaxReconnects(-1))
	if err != nil {
		return fmt.Errorf("nats: connect %s: %w", b.cfg.URL, err)
	}
	defer func() {
		if err != nil {
			nc.Close()
		}
	}()
	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("nats: %w", err)
	}
	var s jetstream.Stream
	if b.cfg.owner() {
		s, err = b.provision(ctx, js)
	} else {
		s, err = b.bind(ctx, js)
	}
	if err != nil {
		return err
	}
	b.conn.Store(&conn{nc: nc, js: js, stream: s})
	return nil
}

func (b *Broker) provision(ctx context.Context, js jetstream.JetStream) (jetstream.Stream, error) {
	s, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       b.cfg.Stream,
		Subjects:   []string{b.cfg.Prefix + ".>"},
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy,
		Duplicates: b.cfg.window(),
		MaxAge:     b.cfg.MaxAge.Duration(),
	})
	if err != nil {
		return nil, fmt.Errorf("nats: provision stream %s: %w", b.cfg.Stream, err)
	}
	return s, nil
}

func (b *Broker) bind(ctx context.Context, js jetstream.JetStream) (jetstream.Stream, error) {
	s, err := js.Stream(ctx, b.cfg.Stream)
	if err != nil {
		return nil, fmt.Errorf("nats: bind stream %s: %w", b.cfg.Stream, err)
	}
	subjects := s.CachedInfo().Config.Subjects
	for _, subj := range subjects {
		if captures(subj, b.cfg.Prefix) {
			return s, nil
		}
	}
	return nil, fmt.Errorf("nats: bind stream %s: its subjects %q do not capture %s.>", b.cfg.Stream, subjects, b.cfg.Prefix)
}

// See docs/internals.md
func captures(subj, prefix string) bool {
	want := strings.Split(prefix, ".")
	for i, tok := range strings.Split(subj, ".") {
		switch {
		case tok == ">":
			return true
		case i >= len(want):
			return false
		case tok != "*" && tok != want[i]:
			return false
		}
	}
	return false
}

// Conn is the native handle, for a use beyond the standard tier, such as
// request and reply: the started broker's connection, or nil before Start.
func (b *Broker) Conn() *natsgo.Conn {
	if c := b.conn.Load(); c != nil {
		return c.nc
	}
	return nil
}

// Ready reports whether the broker is started and its connection is up.
func (b *Broker) Ready() bool {
	c := b.conn.Load()
	return c != nil && c.nc.IsConnected()
}

// Shutdown drains the connection: its subscriptions stop, pending publishes
// and acknowledgements flush, and it closes. It returns once the connection
// is closed, at once for a broker that never started. When ctx ends first,
// it closes the connection without finishing the drain and returns ctx's
// error.
func (b *Broker) Shutdown(ctx context.Context) error {
	c := b.conn.Load()
	if c == nil {
		return nil
	}
	if err := c.nc.Drain(); err != nil && !errors.Is(err, natsgo.ErrConnectionClosed) && !errors.Is(err, natsgo.ErrConnectionReconnecting) {
		c.nc.Close()
		return fmt.Errorf("nats: drain: %w", err)
	}
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for !c.nc.IsClosed() {
		select {
		case <-ctx.Done():
			c.nc.Close()
			return fmt.Errorf("nats: drain: %w", ctx.Err())
		case <-t.C:
		}
	}
	return nil
}

// Publish publishes e in binary content mode with its type as the subject,
// unchanged, and its source and id as Nats-Msg-Id, and returns once the
// stream holds it. A repeat of a source and id within the stream's
// deduplication window is accepted and dropped. Whether or not the broker
// is started, it rejects what [messaging.Encode] rejects, a header value
// holding CR or LF, which the NATS protocol cannot carry, and a type outside
// Prefix, which the stream does not capture; the last is [event.Permanent],
// since no retry brings the type under the prefix. Otherwise it fails with
// [ErrNotStarted] before Start.
func (b *Broker) Publish(ctx context.Context, e event.Event) error {
	h, body, err := messaging.Encode(e)
	if err == nil {
		err = carriable(h)
	}
	if err == nil {
		err = b.namespaced("type", e.Type)
	}
	if err != nil {
		return fmt.Errorf("nats: publish: %w", err)
	}
	c := b.conn.Load()
	if c == nil {
		return ErrNotStarted
	}
	msg := &natsgo.Msg{Subject: e.Type, Header: natsgo.Header(h), Data: body}
	msg.Header.Set(jetstream.MsgIDHeader, msgID(e))
	if _, err := c.js.PublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("nats: publish %s: %w", e.ID, err)
	}
	return nil
}

// Append stores e on subject, as Publish publishes it on its type, when
// subject's last stream sequence is expect, 0 for a subject with no
// message, and returns its stream sequence; it implements
// [messaging.Journal.Append]. The message carries e's source and id as
// Nats-Msg-Id, so a repeat within the stream's deduplication window,
// whether appended or published, returns the original's sequence. When the
// stream reports another last sequence, Append looks up subject's last
// message, and returns its sequence when it is e, so a retry whose first
// acknowledgement was lost succeeds; otherwise it fails wrapping
// [messaging.ErrConflict], naming subject and expect.
//
// Whether or not the broker is started, Append rejects what Publish rejects
// of e, and a subject that breaks [messaging.CheckType]; it refuses a
// subject outside Prefix as [event.Permanent]. Otherwise it fails with
// [ErrNotStarted] before Start.
func (b *Broker) Append(ctx context.Context, subject string, expect uint64, e event.Event) (uint64, error) {
	h, body, err := messaging.Encode(e)
	if err == nil {
		err = carriable(h)
	}
	if err == nil {
		err = messaging.CheckType(subject)
	}
	if err == nil {
		err = b.namespaced("subject", subject)
	}
	if err != nil {
		return 0, fmt.Errorf("nats: append to %q: %w", subject, err)
	}
	c := b.conn.Load()
	if c == nil {
		return 0, ErrNotStarted
	}
	msg := &natsgo.Msg{Subject: subject, Header: natsgo.Header(h), Data: body}
	msg.Header.Set(jetstream.MsgIDHeader, msgID(e))
	ack, err := c.js.PublishMsg(ctx, msg, jetstream.WithExpectLastSequencePerSubject(expect))
	if err == nil {
		return ack.Sequence, nil
	}
	if !wrongLastSequence(err) {
		return 0, fmt.Errorf("nats: append %s to %s: %w", e.ID, subject, err)
	}
	last, lerr := c.stream.GetLastMsgForSubject(ctx, subject)
	if lerr == nil && holds(last, e) {
		return last.Sequence, nil
	}
	conflict := fmt.Errorf("nats: append %s to %s at %d: %w", e.ID, subject, expect, messaging.ErrConflict)
	if lerr != nil && !errors.Is(lerr, jetstream.ErrMsgNotFound) {
		return 0, fmt.Errorf("%w; its last message: %w", conflict, lerr)
	}
	return 0, conflict
}

func wrongLastSequence(err error) bool {
	var apiErr *jetstream.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence ||
		apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant
}

// Read yields subject's events after the stream sequence after, 0 for all,
// in sequence order, each with its stream sequence; it implements
// [messaging.Journal.Read]. It looks up subject's last message once, when
// the iteration begins, and reads through an ordered consumer filtered on
// subject up to that message's sequence, so an event stored during the
// read is not yielded. When that message is removed before the read reaches
// it, the read ends at the subject's last message before it. A subject with no message, or whose last sequence is
// at or before after, yields nothing and creates no consumer. A message that
// does not decode yields its error and ends the read. The consumer is
// deleted when the read ends, whether it reached the end, failed, or was
// stopped early.
//
// Whether or not the broker is started, Read yields as its single error a
// subject that breaks [messaging.CheckType], and a subject outside Prefix
// as [event.Permanent]; otherwise it yields [ErrNotStarted] before Start.
func (b *Broker) Read(ctx context.Context, subject string, after uint64) iter.Seq2[messaging.Entry, error] {
	return func(yield func(messaging.Entry, error) bool) {
		if err := b.read(ctx, subject, after, yield); err != nil {
			yield(messaging.Entry{}, err)
		}
	}
}

func (b *Broker) read(ctx context.Context, subject string, after uint64, yield func(messaging.Entry, error) bool) error {
	err := messaging.CheckType(subject)
	if err == nil {
		err = b.namespaced("subject", subject)
	}
	if err != nil {
		return fmt.Errorf("nats: read %q: %w", subject, err)
	}
	c := b.conn.Load()
	if c == nil {
		return ErrNotStarted
	}
	last, err := c.stream.GetLastMsgForSubject(ctx, subject)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("nats: read %s: its last message: %w", subject, err)
	}
	if last.Sequence <= after {
		return nil
	}
	cons, err := c.stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{subject},
		DeliverPolicy:  jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:    after + 1,
	})
	if err != nil {
		return fmt.Errorf("nats: read %s: %w", subject, err)
	}
	defer release(ctx, c.stream, cons)
	msgs, err := cons.Messages()
	if err != nil {
		return fmt.Errorf("nats: read %s: %w", subject, err)
	}
	defer msgs.Stop()
	for {
		m, err := msgs.Next(jetstream.NextContext(ctx))
		if err != nil {
			return fmt.Errorf("nats: read %s: %w", subject, err)
		}
		meta, err := m.Metadata()
		if err != nil {
			return fmt.Errorf("nats: read %s: %w", subject, err)
		}
		seq := meta.Sequence.Stream
		if seq > last.Sequence {
			return nil
		}
		e, err := event.Decode(event.Header(m.Headers()), m.Data())
		if err != nil {
			return fmt.Errorf("nats: read %s: message %d: %w", subject, seq, err)
		}
		if !yield(messaging.Entry{Event: e, Sequence: seq}, nil) || seq == last.Sequence || meta.NumPending == 0 {
			return nil
		}
	}
}

func release(ctx context.Context, s jetstream.Stream, cons jetstream.Consumer) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseWait)
	defer cancel()
	_ = s.DeleteConsumer(ctx, cons.CachedInfo().Name)
}

func holds(m *jetstream.RawStreamMsg, e event.Event) bool {
	got, err := event.Decode(event.Header(m.Header), m.Data)
	return err == nil && got.Source == e.Source && got.ID == e.ID
}

func msgID(e event.Event) string {
	return strconv.Itoa(len(e.Source)) + ":" + e.Source + e.ID
}

func carriable(h event.Header) error {
	for name, vs := range h {
		for _, v := range vs {
			if strings.ContainsAny(v, "\r\n") {
				return fmt.Errorf("header %s: a value must not contain CR or LF", name)
			}
		}
	}
	return nil
}

func (b *Broker) namespaced(what, name string) error {
	if !strings.HasPrefix(name, b.cfg.Prefix+".") {
		return event.Permanent(fmt.Errorf("%s %q is not under the prefix %q", what, name, b.cfg.Prefix))
	}
	return nil
}
