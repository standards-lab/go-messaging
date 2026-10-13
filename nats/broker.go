package nats

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	natsgo "github.com/nats-io/nats.go"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-core/reactor"
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

// conn is the connection a started broker owns.
type conn struct {
	nc *natsgo.Conn
}

var _ messaging.Broker = (*Broker)(nil)

// New checks cfg and returns a broker on it, unconnected. It does no I/O.
func New(cfg Config) (*Broker, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Broker{cfg: cfg}, nil
}

// Start connects to the server, waiting until ctx's deadline or 10s. Once
// up, the connection reconnects without limit. A failed Start closes what it
// opened, and the broker can start again. A broker starts once: a Start
// while another is in progress or has succeeded fails with [ErrStarted] and
// opens nothing.
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
	b.conn.Store(&conn{nc: nc})
	return nil
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

// errUnimplemented is the failure of a call whose body a later slice of the
// nats task ports; it is removed with the last of them.
var errUnimplemented = errors.New("nats: not implemented")

// Publish publishes e. It fails with [ErrNotStarted] before Start.
func (b *Broker) Publish(_ context.Context, _ event.Event) error {
	if b.conn.Load() == nil {
		return ErrNotStarted
	}
	return errUnimplemented
}

// Subscribe validates sub and returns a source on it. The source passes
// report each error it survives; a nil report discards them.
func (b *Broker) Subscribe(sub messaging.Subscription, _ messaging.Report) (reactor.Source[event.Event], error) {
	if err := sub.Validate(); err != nil {
		return nil, err
	}
	return &source{b: b}, nil
}

// source is one member of a durable consumer: the loop a reactor runs.
type source struct {
	b *Broker
}

// Receive fails with [ErrNotStarted] before the broker's Start.
func (s *source) Receive(_ context.Context, _ reactor.Func[event.Event]) error {
	if s.b.conn.Load() == nil {
		return ErrNotStarted
	}
	return errUnimplemented
}

// Ready reports whether the source is receiving.
func (s *source) Ready() bool { return false }
