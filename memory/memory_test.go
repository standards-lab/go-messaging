package memory_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-core/reactor"
	"github.com/standards-lab/go-messaging"
	"github.com/standards-lab/go-messaging/memory"
	"github.com/standards-lab/go-messaging/messagingtest"
)

func TestConformance(t *testing.T) {
	messagingtest.Run(t, func(*testing.T) messaging.Broker { return memory.New() })
}

// A late error, after another member has already handled the redelivery,
// must not schedule a third delivery.
func TestLateOutcomeIgnored(t *testing.T) {
	b := memory.New()
	sub := messaging.Subscription{Name: "late", AckWait: 50 * time.Millisecond}
	var calls atomic.Int32
	release := make(chan struct{})
	var wg sync.WaitGroup
	handler := func(ctx context.Context, _ event.Event) error {
		if calls.Add(1) == 1 {
			<-release // outlive AckWait, ignoring the deadline
			return errors.New("late failure")
		}
		return nil
	}
	var rs []*reactor.Reactor[event.Event]
	for range 2 {
		src, err := b.Subscribe(sub, nil)
		if err != nil {
			t.Fatal(err)
		}
		r := reactor.New(src, handler)
		if err := r.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		rs = append(rs, r)
	}
	wg.Go(func() {
		deadline := time.Now().Add(2 * time.Second)
		for calls.Load() < 2 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		close(release)
	})
	if err := b.Publish(context.Background(), event.Event{ID: "x", Source: "/t", Type: "t"}); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	time.Sleep(200 * time.Millisecond)
	if n := calls.Load(); n != 2 {
		t.Errorf("handled %d times, want 2: the late failure must be ignored", n)
	}
	for _, r := range rs {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := r.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
		cancel()
	}
}

func TestBindingComparesMeaning(t *testing.T) {
	b := memory.New()
	types := []string{"b", "a"}
	if _, err := b.Subscribe(messaging.Subscription{Name: "n", Types: types}, nil); err != nil {
		t.Fatal(err)
	}
	types[0] = "changed" // the consumer must keep its own copy
	if _, err := b.Subscribe(messaging.Subscription{Name: "n", Types: []string{"a", "b"}, AckWait: messaging.DefaultAckWait}, nil); err != nil {
		t.Errorf("the same types in another order, with the default AckWait spelled out, must bind: %v", err)
	}
	if _, err := b.Subscribe(messaging.Subscription{Name: "empty", Types: []string{}}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Subscribe(messaging.Subscription{Name: "empty"}, nil); err != nil {
		t.Errorf("nil and empty Types mean the same filter: %v", err)
	}
}

type ctxKey struct{}

// A message that cannot decode is reported with the context the source
// receives under, and terminated, and the next event still delivers.
func TestUndecodableReportedAndTerminated(t *testing.T) {
	b := memory.New()
	type reported struct {
		ctx context.Context
		err error
	}
	reports := make(chan reported, 4)
	src, err := b.Subscribe(messaging.Subscription{Name: "corrupt", RetryDelay: time.Millisecond}, func(ctx context.Context, err error) {
		reports <- reported{ctx, err}
	})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var handled []string
	got := make(chan struct{})
	r := reactor.New(src, func(_ context.Context, e event.Event) error {
		mu.Lock()
		handled = append(handled, e.ID)
		mu.Unlock()
		close(got)
		return nil
	})
	if err := r.Start(context.WithValue(context.Background(), ctxKey{}, "receive")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shutdown(t, r) })

	b.AppendRaw("t", event.Header{}, []byte("not an event"))
	if err := b.Publish(context.Background(), event.Event{ID: "after", Source: "/t", Type: "t"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the event after the corrupt one")
	}
	select {
	case rep := <-reports:
		if rep.err == nil {
			t.Error("reported a nil error")
		}
		if v, _ := rep.ctx.Value(ctxKey{}).(string); v != "receive" {
			t.Error("the report's context is not the one the source receives under")
		}
	default:
		t.Fatal("the undecodable message was not reported")
	}
	time.Sleep(100 * time.Millisecond)
	if n := len(reports); n != 0 {
		t.Errorf("reported %d more times, want once: the message must be terminated", n)
	}
	mu.Lock()
	defer mu.Unlock()
	if want := []string{"after"}; len(handled) != 1 || handled[0] != want[0] {
		t.Errorf("handled %v, want %v", handled, want)
	}
}

// A nil report discards what the source survives; the source keeps going.
func TestNilReportDiscards(t *testing.T) {
	b := memory.New()
	src, err := b.Subscribe(messaging.Subscription{Name: "discard"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan string, 1)
	r := reactor.New(src, func(_ context.Context, e event.Event) error {
		got <- e.ID
		return nil
	})
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shutdown(t, r) })
	b.AppendRaw("t", event.Header{}, []byte("not an event"))
	if err := b.Publish(context.Background(), event.Event{ID: "after", Source: "/t", Type: "t"}); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-got:
		if id != "after" {
			t.Errorf("handled %q, want after", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the event after the corrupt one")
	}
}

func shutdown(t *testing.T, r *reactor.Reactor[event.Event]) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}
