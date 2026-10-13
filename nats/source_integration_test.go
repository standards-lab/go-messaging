//go:build integration

package nats_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-core/reactor"

	"github.com/standards-lab/go-messaging"
	"github.com/standards-lab/go-messaging/messagingtest"
	"github.com/standards-lab/go-messaging/nats"
	"github.com/standards-lab/go-messaging/nats/natstest"
)

// TestConformance runs the base contract's suite on an owner broker, each
// case on its own scratch stream.
func TestConformance(t *testing.T) {
	url := natstest.Open(t)
	messagingtest.Run(t, func(t *testing.T) messaging.Broker {
		stream, prefix := natstest.Scratch(t, url)
		return started(t, nats.Config{URL: url, Stream: stream, Prefix: prefix, Owner: owner()})
	})
}

// ownerBroker is a started owner broker on a scratch stream, with the
// stream's name and prefix.
func ownerBroker(t *testing.T) (b *nats.Broker, url, stream, prefix string) {
	t.Helper()
	url = natstest.Open(t)
	stream, prefix = natstest.Scratch(t, url)
	b = started(t, nats.Config{URL: url, Stream: stream, Prefix: prefix, Owner: owner()})
	return b, url, stream, prefix
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(failsafe)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// reports collects the errors a source reports.
type reports struct {
	mu   sync.Mutex
	errs []error
}

func (r *reports) report(_ context.Context, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, err)
}

func (r *reports) seen() []error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]error(nil), r.errs...)
}

// member subscribes under sub with report, starts a reactor on the source,
// and waits until it is receiving. The cleanup shuts it down, before the
// broker's own cleanup, which registered earlier.
func member(t *testing.T, b *nats.Broker, sub messaging.Subscription, report messaging.Report, fn reactor.Func[event.Event]) {
	t.Helper()
	src, err := b.Subscribe(sub, report)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	r := reactor.New(src, fn)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), failsafe)
		defer cancel()
		_ = r.Shutdown(ctx)
	})
	eventually(t, "the member to be receiving", r.Ready)
}

// receive runs src's Receive with fn in the background, returning the
// channel its result arrives on.
func receive(t *testing.T, src reactor.Source[event.Event], fn reactor.Func[event.Event]) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- src.Receive(t.Context(), fn) }()
	return done
}

// ended waits for Receive's result, failing the test when it does not come.
func ended(t *testing.T, done <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(failsafe):
		t.Fatalf("Receive did not end after %s", what)
		return nil
	}
}

// consumerInfo is the durable's current state.
func consumerInfo(t *testing.T, js jetstream.JetStream, stream, name string) *jetstream.ConsumerInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	c, err := js.Consumer(ctx, stream, name)
	if err != nil {
		t.Fatalf("consumer %s: %v", name, err)
	}
	info, err := c.Info(ctx)
	if err != nil {
		t.Fatalf("consumer %s info: %v", name, err)
	}
	return info
}

// The first Receive binds the durable on a config derived from the
// normalized subscription.
func TestSourceBindsConsumer(t *testing.T) {
	b, url, stream, prefix := ownerBroker(t)
	sub := messaging.Subscription{Name: "derived", Types: []string{"b", "a", "a"}, MaxDeliver: 4, AckWait: 2 * time.Second, Start: messaging.StartNew}
	member(t, b, sub, nil, func(context.Context, event.Event) error { return nil })
	member(t, b, messaging.Subscription{Name: "defaults"}, nil, func(context.Context, event.Event) error { return nil })
	js := jetStream(t, url)

	got := consumerInfo(t, js, stream, "derived").Config
	if got.Durable != "derived" || got.DeliverPolicy != jetstream.DeliverNewPolicy || got.AckPolicy != jetstream.AckExplicitPolicy {
		t.Errorf("Durable, DeliverPolicy, AckPolicy = %s, %v, %v; want derived, new, explicit", got.Durable, got.DeliverPolicy, got.AckPolicy)
	}
	if want := 2*time.Second + nats.AckMargin; got.AckWait != want {
		t.Errorf("AckWait = %v, want %v", got.AckWait, want)
	}
	if got.MaxDeliver != 4 {
		t.Errorf("MaxDeliver = %d, want 4", got.MaxDeliver)
	}
	if want := []string{prefix + ".a", prefix + ".b"}; len(got.FilterSubjects) != 2 || got.FilterSubjects[0] != want[0] || got.FilterSubjects[1] != want[1] {
		t.Errorf("FilterSubjects = %q, want %q", got.FilterSubjects, want)
	}

	got = consumerInfo(t, js, stream, "defaults").Config
	if got.DeliverPolicy != jetstream.DeliverAllPolicy {
		t.Errorf("default DeliverPolicy = %v, want all", got.DeliverPolicy)
	}
	if want := messaging.DefaultAckWait + nats.AckMargin; got.AckWait != want {
		t.Errorf("default AckWait = %v, want %v", got.AckWait, want)
	}
	if got.MaxDeliver != -1 {
		t.Errorf("default MaxDeliver = %d, want -1, unlimited", got.MaxDeliver)
	}
	if len(got.FilterSubjects) != 0 || got.FilterSubject != "" {
		t.Errorf("default filter = %q %q, want none", got.FilterSubject, got.FilterSubjects)
	}
}

// A subscription under an existing Name with a different config fails its
// Receive, and leaves the consumer as the first made it.
func TestSourceDifferentConfigFails(t *testing.T) {
	b, url, stream, _ := ownerBroker(t)
	member(t, b, messaging.Subscription{Name: "fixed", MaxDeliver: 3}, nil, func(context.Context, event.Event) error { return nil })
	src, err := b.Subscribe(messaging.Subscription{Name: "fixed", MaxDeliver: 5}, nil)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	err = ended(t, receive(t, src, func(context.Context, event.Event) error { return nil }), "a failed binding")
	if err == nil {
		t.Fatal("Receive bound a consumer of another config")
	}
	t.Logf("Receive: %v", err)
	if src.Ready() {
		t.Error("a source whose binding failed reports ready")
	}
	if got := consumerInfo(t, jetStream(t, url), stream, "fixed").Config.MaxDeliver; got != 3 {
		t.Errorf("MaxDeliver = %d after the failed binding, want the first config's 3", got)
	}
}

// An outcome settled after the handler's deadline, but before the
// consumer's AckWait, which is AckMargin longer, is dropped: had the
// acknowledgement been sent, the server would have taken it and never
// redelivered the event.
func TestSourceDropsLateOutcome(t *testing.T) {
	b, _, _, _ := ownerBroker(t)
	var mu sync.Mutex
	var attempts []int
	member(t, b, messaging.Subscription{Name: "late", AckWait: 200 * time.Millisecond}, nil, func(ctx context.Context, _ event.Event) error {
		mu.Lock()
		attempts = append(attempts, messaging.Attempt(ctx))
		first := len(attempts) == 1
		mu.Unlock()
		if first {
			<-ctx.Done()
			time.Sleep(nats.AckMargin / 5) // late, but within the margin
		}
		return nil
	})
	publish(t, b, event.Event{ID: "l", Source: "/test", Type: "t"})
	eventually(t, "the redelivery of the late event", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(attempts) >= 2
	})
	mu.Lock()
	defer mu.Unlock()
	if attempts[0] != 1 || attempts[1] != 2 {
		t.Errorf("attempts %v, want 1 then 2", attempts)
	}
}

// A message that does not decode is reported, terminated so it is never
// delivered again, and passed over for the next.
func TestSourceReportsUndecodable(t *testing.T) {
	b, url, stream, prefix := ownerBroker(t)
	var r reports
	var mu sync.Mutex
	var handled []string
	sub := messaging.Subscription{Name: "strict", AckWait: 100 * time.Millisecond}
	member(t, b, sub, r.report, func(_ context.Context, e event.Event) error {
		mu.Lock()
		handled = append(handled, e.ID)
		mu.Unlock()
		return nil
	})
	js := jetStream(t, url)
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	if _, err := js.Publish(ctx, prefix+".t", []byte("not an event")); err != nil {
		t.Fatalf("publish raw bytes: %v", err)
	}
	publish(t, b, event.Event{ID: "after", Source: "/test", Type: "t"})
	eventually(t, "the event after the undecodable one", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(handled) == 1
	})
	// Past the consumer's AckWait, an unsettled message would be back.
	time.Sleep(3 * (sub.AckWait + nats.AckMargin))
	errs := r.seen()
	if len(errs) != 1 {
		t.Fatalf("reported %v, want the undecodable message once", errs)
	}
	t.Logf("reported: %v", errs[0])
	info := consumerInfo(t, js, stream, "strict")
	if info.NumAckPending != 0 || info.NumRedelivered != 0 {
		t.Errorf("ack pending %d, redelivered %d; want 0, 0: the message terminated", info.NumAckPending, info.NumRedelivered)
	}
	if info.AckFloor.Stream != 2 {
		t.Errorf("ack floor %d, want 2: both messages settled", info.AckFloor.Stream)
	}
}

// A member whose consumer is deleted while it handles a delivery, so no
// pull is waiting to hear of it, reports the acknowledgement that fails to
// send, then ends Receive once its next pulls fail.
func TestSourceDeletedConsumerEndsReceive(t *testing.T) {
	b, url, stream, _ := ownerBroker(t)
	var r reports
	src, err := b.Subscribe(messaging.Subscription{Name: "doomed"}, r.report)
	if err != nil {
		t.Fatal(err)
	}
	handling, release := make(chan struct{}), make(chan struct{})
	done := receive(t, src, func(context.Context, event.Event) error {
		close(handling)
		<-release
		return nil
	})
	eventually(t, "the source to be ready", src.Ready)
	publish(t, b, event.Event{ID: "d", Source: "/test", Type: "t"})
	select {
	case <-handling:
	case <-time.After(failsafe):
		t.Fatal("timed out waiting for the handler")
	}
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	if err := jetStream(t, url).DeleteConsumer(ctx, stream, "doomed"); err != nil {
		t.Fatal(err)
	}
	close(release)
	err = ended(t, done, "its consumer was deleted")
	if err == nil {
		t.Error("Receive returned nil after its consumer was deleted")
	}
	t.Logf("Receive: %v", err)
	if src.Ready() {
		t.Error("the source reports ready after Receive ended")
	}
	errs := r.seen()
	if len(errs) == 0 {
		t.Error("the acknowledgement to a deleted consumer was not reported")
	}
	for _, e := range errs {
		t.Logf("reported: %v", e)
	}
}

// A closed connection ends Receive with its error.
func TestSourceClosedConnectionEndsReceive(t *testing.T) {
	b, _, _, _ := ownerBroker(t)
	src, err := b.Subscribe(messaging.Subscription{Name: "cut"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	done := receive(t, src, func(context.Context, event.Event) error { return nil })
	eventually(t, "the source to be ready", src.Ready)
	b.Conn().Close()
	err = ended(t, done, "the connection closed")
	if !errors.Is(err, natsgo.ErrConnectionClosed) {
		t.Errorf("Receive = %v, want ErrConnectionClosed", err)
	}
	if b.Ready() {
		t.Error("the broker reports ready on a closed connection")
	}
}

// A non-owner's member receives the events the owner publishes.
func TestSourceNonOwnerReceives(t *testing.T) {
	own, url, stream, prefix := ownerBroker(t)
	other := started(t, nats.Config{URL: url, Stream: stream, Prefix: prefix})
	got := make(chan string, 1)
	member(t, other, messaging.Subscription{Name: "follower"}, nil, func(_ context.Context, e event.Event) error {
		got <- e.ID
		return nil
	})
	publish(t, own, event.Event{ID: "shared", Source: "/test", Type: "t"})
	select {
	case id := <-got:
		if id != "shared" {
			t.Errorf("received %s, want shared", id)
		}
	case <-time.After(failsafe):
		t.Fatal("the non-owner did not receive the owner's event")
	}
}

// Shutdown waits on the drain until ctx ends, then closes the connection
// and returns ctx's error. The native handle's subscription, whose handler
// is still running, holds the drain open.
func TestShutdownClosesWhenCtxEnds(t *testing.T) {
	b, _, _, prefix := ownerBroker(t)
	nc := b.Conn()
	handling, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	if _, err := nc.Subscribe(prefix+".held", func(*natsgo.Msg) {
		close(handling)
		<-release
	}); err != nil {
		t.Fatal(err)
	}
	if err := nc.Publish(prefix+".held", nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handling:
	case <-time.After(failsafe):
		t.Fatal("timed out waiting for the handler")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if err := b.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Shutdown = %v, want the ctx's DeadlineExceeded", err)
	}
	if !nc.IsClosed() {
		t.Error("the connection is open after Shutdown returned")
	}
	if b.Ready() {
		t.Error("the broker reports ready after Shutdown")
	}
}
