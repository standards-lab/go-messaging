package nats_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/standards-lab/go-core/event"

	"github.com/standards-lab/go-messaging"
	"github.com/standards-lab/go-messaging/nats"
)

// closedURL is a NATS URL on a loopback port nothing listens on.
func closedURL(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := lis.Addr().(*net.TCPAddr).Port
	if err := lis.Close(); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("nats://127.0.0.1:%d", port)
}

func newBroker(t *testing.T, url string) *nats.Broker {
	t.Helper()
	b, err := nats.New(nats.Config{URL: url, Stream: "test", Prefix: "test"})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Before Start, the broker is not ready and has no connection, and a
// publish or a receive fails rather than reach a connection that does not
// exist; the coordinator's stage order keeps both from happening in a
// process.
func TestBrokerBeforeStart(t *testing.T) {
	b := newBroker(t, closedURL(t))
	if b.Ready() {
		t.Error("an unstarted broker is ready")
	}
	if b.Conn() != nil {
		t.Error("an unstarted broker has a connection")
	}
	if err := b.Publish(t.Context(), event.Event{ID: "1", Source: "/test", Type: "test.t"}); !errors.Is(err, nats.ErrNotStarted) {
		t.Errorf("Publish = %v, want ErrNotStarted", err)
	}
	if err := b.Shutdown(t.Context()); err != nil {
		t.Errorf("Shutdown of an unstarted broker = %v", err)
	}

	src, err := b.Subscribe(messaging.Subscription{Name: "orders"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if src.Ready() {
		t.Error("a source is ready before it receives")
	}
	err = src.Receive(t.Context(), func(context.Context, event.Event) error { return nil })
	if !errors.Is(err, nats.ErrNotStarted) {
		t.Errorf("Receive = %v, want ErrNotStarted", err)
	}
}

// Subscribe validates the subscription.
func TestBrokerSubscribeValidates(t *testing.T) {
	b := newBroker(t, closedURL(t))
	for name, sub := range map[string]messaging.Subscription{
		"no name":          {},
		"dotted name":      {Name: "not.a.token"},
		"wildcard type":    {Name: "orders", Types: []string{"a.*"}},
		"negative ackwait": {Name: "orders", AckWait: -1},
	} {
		if _, err := b.Subscribe(sub, nil); err == nil {
			t.Errorf("%s: Subscribe accepted %+v", name, sub)
		}
	}
}

// A broker whose server is unreachable fails its Start, which fails the
// process's startup, stays not ready, and can start again.
func TestBrokerStartFailsWithoutNATS(t *testing.T) {
	b := newBroker(t, closedURL(t))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := b.Start(ctx); err == nil {
		t.Fatal("Start succeeded against a closed port")
	}
	if b.Ready() || b.Conn() != nil {
		t.Error("a broker that failed to start is ready, or has a connection")
	}
	if err := b.Start(ctx); err == nil || errors.Is(err, nats.ErrStarted) {
		t.Errorf("a Start after a failed one = %v, want the connection's failure", err)
	}
}

// A Start while another is in progress fails with ErrStarted. The server is
// a listener that accepts and never speaks, so the first Start waits for its
// deadline.
func TestBrokerStartWhileStarting(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := lis.Accept()
		if err == nil {
			accepted <- c
		}
	}()

	b := newBroker(t, "nats://"+lis.Addr().String())
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	first := make(chan error, 1)
	go func() { first <- b.Start(ctx) }()

	select {
	case c := <-accepted:
		t.Cleanup(func() { _ = c.Close() })
	case <-ctx.Done():
		t.Fatal("the first Start never connected")
	}
	if err := b.Start(ctx); !errors.Is(err, nats.ErrStarted) {
		t.Errorf("a Start while another is in progress = %v, want ErrStarted", err)
	}
	if err := <-first; err == nil {
		t.Fatal("Start succeeded against a server that never speaks")
	}
}

// Publish rejects an event messaging.Encode rejects, and a header value the
// NATS protocol cannot carry, before it needs a connection.
func TestPublishRejects(t *testing.T) {
	b := newBroker(t, closedURL(t))
	for name, e := range map[string]event.Event{
		"no id":         {Source: "/test", Type: "test.t"},
		"wildcard type": {ID: "1", Source: "/test", Type: "test.t.*"},
		"CR LF":         {ID: "1", Source: "/test", Type: "test.t", Extensions: map[string]string{"note": "two\r\nlines"}},
		"LF":            {ID: "1", Source: "/test", Type: "test.t", Extensions: map[string]string{"note": "two\nlines"}},
		"CR":            {ID: "1", Source: "/test", Type: "test.t", Extensions: map[string]string{"note": "two\rlines"}},
		"CR in subject": {ID: "1", Source: "/test", Type: "test.t", Subject: "a\rb"},
	} {
		err := b.Publish(t.Context(), e)
		if err == nil || errors.Is(err, nats.ErrNotStarted) {
			t.Errorf("%s: Publish = %v, want a rejection", name, err)
		}
	}
}

// Publish refuses a type that is not under the prefix as permanent, naming
// the type and the prefix, before it needs a connection: the prefix itself,
// and a type whose first token only starts with the prefix, are outside it.
func TestPublishRefusesTypeOutsidePrefix(t *testing.T) {
	b := newBroker(t, closedURL(t))
	for _, typ := range []string{"orders.created", "test", "testing.created", "other.test.created"} {
		err := b.Publish(t.Context(), event.Event{ID: "1", Source: "/test", Type: typ})
		if err == nil || errors.Is(err, nats.ErrNotStarted) {
			t.Errorf("%s: Publish = %v, want a refusal", typ, err)
			continue
		}
		if !event.IsPermanent(err) {
			t.Errorf("%s: Publish = %v, want a permanent error", typ, err)
		}
		if msg := err.Error(); !strings.Contains(msg, strconv.Quote(typ)) || !strings.Contains(msg, `"test"`) {
			t.Errorf("%s: Publish = %v, want it to name the type and the prefix test", typ, err)
		}
	}
	if err := b.Publish(t.Context(), event.Event{ID: "1", Source: "/test", Type: "test.created"}); !errors.Is(err, nats.ErrNotStarted) {
		t.Errorf("test.created: Publish = %v, want ErrNotStarted: the type is under the prefix", err)
	}
}

// Subscribe refuses a subscription naming a type that is not under the
// prefix, which the stream never captures, naming the type and the prefix.
func TestSubscribeRefusesTypeOutsidePrefix(t *testing.T) {
	b := newBroker(t, closedURL(t))
	for _, typ := range []string{"orders.created", "test", "testing.created"} {
		_, err := b.Subscribe(messaging.Subscription{Name: "orders", Types: []string{"test.created", typ}}, nil)
		if err == nil {
			t.Errorf("%s: Subscribe accepted the type", typ)
			continue
		}
		if msg := err.Error(); !strings.Contains(msg, strconv.Quote(typ)) || !strings.Contains(msg, `"test"`) {
			t.Errorf("%s: Subscribe = %v, want it to name the type and the prefix test", typ, err)
		}
	}
	if _, err := b.Subscribe(messaging.Subscription{Name: "orders", Types: []string{"test.created", "test.paid"}}, nil); err != nil {
		t.Errorf("Subscribe to types under the prefix: %v", err)
	}
}
