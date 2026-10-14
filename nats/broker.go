package nats

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-messaging"
)

// connectWait bounds Start's connection when ctx has no deadline.
const connectWait = 10 * time.Second

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

// conn is the connection a started broker owns, and its JetStream context.
type conn struct {
	nc *natsgo.Conn
	js jetstream.JetStream
}

var _ messaging.Broker = (*Broker)(nil)

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
	if b.cfg.owner() {
		err = b.provision(ctx, js)
	} else {
		err = b.bind(ctx, js)
	}
	if err != nil {
		return err
	}
	b.conn.Store(&conn{nc: nc, js: js})
	return nil
}

// provision creates the stream, or updates it to the owner's configuration.
func (b *Broker) provision(ctx context.Context, js jetstream.JetStream) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       b.cfg.Stream,
		Subjects:   []string{b.cfg.Prefix + ".>"},
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy,
		Duplicates: b.cfg.window(),
		MaxAge:     b.cfg.MaxAge.Duration(),
	})
	if err != nil {
		return fmt.Errorf("nats: provision stream %s: %w", b.cfg.Stream, err)
	}
	return nil
}

// bind looks the stream up, as a non-owner does, and checks that it
// captures every subject under Prefix, so every event the broker publishes
// lands in it.
func (b *Broker) bind(ctx context.Context, js jetstream.JetStream) error {
	s, err := js.Stream(ctx, b.cfg.Stream)
	if err != nil {
		return fmt.Errorf("nats: bind stream %s: %w", b.cfg.Stream, err)
	}
	subjects := s.CachedInfo().Config.Subjects
	for _, subj := range subjects {
		if captures(subj, b.cfg.Prefix) {
			return nil
		}
	}
	return fmt.Errorf("nats: bind stream %s: its subjects %q do not capture %s.>", b.cfg.Stream, subjects, b.cfg.Prefix)
}

// captures reports whether the stream subject subj matches every subject
// under prefix, that is, every subject Prefix.> matches. Under NATS's
// wildcard rules, '*' matches one token and '>', as the last token, one or
// more. So subj captures when, token by token, each of its tokens before
// the prefix's end is '*' or the prefix's own token, and it ends in '>' no
// later than the token after the prefix: Prefix.> itself, a parent's .>, or
// '>'. A '*' in the token after the prefix captures only one more token,
// and a subject without '>' only a fixed number, so neither captures.
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
	// A connection that is reconnecting cannot drain, and Drain closes it.
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
		err = b.namespaced(e.Type)
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

// msgID is e's deduplication key: its source and id, which together
// identify a CloudEvents event. The source is length-prefixed, so no source
// and id pair can spell another's key.
func msgID(e event.Event) string {
	return strconv.Itoa(len(e.Source)) + ":" + e.Source + e.ID
}

// carriable fails on a header value the NATS protocol cannot carry.
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

// namespaced fails, as [event.Permanent], on a type outside Prefix: one
// that does not start with Prefix and a further token. The type is the
// subject, and the stream captures Prefix.>, which matches one or more
// tokens after the prefix, so the prefix itself is outside it too.
func (b *Broker) namespaced(typ string) error {
	if !strings.HasPrefix(typ, b.cfg.Prefix+".") {
		return event.Permanent(fmt.Errorf("type %q is not under the prefix %q", typ, b.cfg.Prefix))
	}
	return nil
}
