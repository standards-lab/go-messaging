//go:build integration

package nats_test

import (
	"context"
	"errors"
	neturl "net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-core/process/processtest"
	"github.com/standards-lab/go-core/reactor"

	"github.com/standards-lab/go-messaging"
	"github.com/standards-lab/go-messaging/messagingtest"
	"github.com/standards-lab/go-messaging/nats"
	"github.com/standards-lab/go-messaging/nats/natstest"
)

// TestConformance runs the base contract's suite on an owner broker, each
// case on its own scratch stream. The suite's types name no namespace, so
// they reach the broker under the scratch prefix, as a service's own types
// sit under its prefix.
func TestConformance(t *testing.T) {
	url := natstest.Open(t)
	messagingtest.Run(t, func(t *testing.T) messaging.Broker {
		stream, prefix := natstest.Scratch(t, url)
		b := started(t, nats.Config{URL: url, Stream: stream, Prefix: prefix, Owner: new(true)})
		return namespaced{Broker: b, prefix: prefix}
	})
}

// namespaced is a broker whose callers' types are relative to prefix: it
// publishes and subscribes to prefix.<type>, and delivers each event with
// its type relative again. An empty type stays empty, so an event without
// one is refused for that, as the suite expects.
type namespaced struct {
	*nats.Broker
	prefix string
}

func (n namespaced) Publish(ctx context.Context, e event.Event) error {
	if e.Type != "" {
		e.Type = n.prefix + "." + e.Type
	}
	return n.Broker.Publish(ctx, e)
}

func (n namespaced) Subscribe(sub messaging.Subscription, report messaging.Report) (reactor.Source[event.Event], error) {
	types := make([]string, len(sub.Types))
	for i, typ := range sub.Types {
		types[i] = n.prefix + "." + typ
	}
	sub.Types = types
	src, err := n.Broker.Subscribe(sub, report)
	if err != nil {
		return nil, err
	}
	return relative{Source: src, prefix: n.prefix}, nil
}

// relative is a source that delivers each event with its type relative to
// prefix.
type relative struct {
	reactor.Source[event.Event]
	prefix string
}

func (r relative) Receive(ctx context.Context, fn reactor.Func[event.Event]) error {
	return r.Source.Receive(ctx, func(ctx context.Context, e event.Event) error {
		e.Type = strings.TrimPrefix(e.Type, r.prefix+".")
		return fn(ctx, e)
	})
}

// ownerBroker is a started owner broker on a scratch stream, with the
// stream's name and prefix.
func ownerBroker(t *testing.T) (b *nats.Broker, url, stream, prefix string) {
	t.Helper()
	url = natstest.Open(t)
	stream, prefix = natstest.Scratch(t, url)
	b = started(t, nats.Config{URL: url, Stream: stream, Prefix: prefix, Owner: new(true)})
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
	sub := messaging.Subscription{Name: "derived", Types: []string{prefix + ".b", prefix + ".a", prefix + ".a"}, MaxDeliver: 4, AckWait: 2 * time.Second, Start: messaging.StartNew}
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
	b, _, _, prefix := ownerBroker(t)
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
	publish(t, b, event.Event{ID: "l", Source: "/test", Type: prefix + ".t"})
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
	publish(t, b, event.Event{ID: "after", Source: "/test", Type: prefix + ".t"})
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
	b, url, stream, prefix := ownerBroker(t)
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
	publish(t, b, event.Event{ID: "d", Source: "/test", Type: prefix + ".t"})
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
	// Failed pulls are reported too, so only a report that names the
	// acknowledgement proves its failure reached the report function.
	acked := false
	for _, e := range r.seen() {
		t.Logf("reported: %v", e)
		acked = acked || strings.Contains(e.Error(), "acknowledge")
	}
	if !acked {
		t.Error("the acknowledgement to a deleted consumer was not reported")
	}
}

// A deleted stream ends Receive with its error: the consumer's deletion,
// which a waiting pull hears of, or the stream's absence, which the member
// learns by asking once its pulls fail. A deletion that lands between one
// pull's expiry and the next is learned by asking, so a waiting pull accepts
// either error.
func TestSourceDeletedStreamEndsReceive(t *testing.T) {
	for name, tc := range map[string]struct {
		hold bool // the handler holds a delivery, so no pull is waiting
		want []error
	}{
		"while pulling":  {want: []error{jetstream.ErrConsumerDeleted, jetstream.ErrStreamNotFound}},
		"while handling": {hold: true, want: []error{jetstream.ErrStreamNotFound}},
	} {
		t.Run(name, func(t *testing.T) {
			b, url, stream, prefix := ownerBroker(t)
			src, err := b.Subscribe(messaging.Subscription{Name: "orphan"}, nil)
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
			if tc.hold {
				publish(t, b, event.Event{ID: "o", Source: "/test", Type: prefix + ".t"})
				select {
				case <-handling:
				case <-time.After(failsafe):
					t.Fatal("timed out waiting for the handler")
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), failsafe)
			defer cancel()
			if err := jetStream(t, url).DeleteStream(ctx, stream); err != nil {
				t.Fatal(err)
			}
			close(release)
			err = ended(t, done, "its stream was deleted")
			if !slices.ContainsFunc(tc.want, func(w error) bool { return errors.Is(err, w) }) {
				t.Errorf("Receive = %v, want one of %v", err, tc.want)
			}
			if src.Ready() {
				t.Error("the source reports ready after Receive ended")
			}
		})
	}
}

// A handler error under a zero RetryDelay is nak'd at once: the event comes
// back promptly, long before the consumer's AckWait would return it
// unsettled.
func TestSourceNaksAtOnceWithoutRetryDelay(t *testing.T) {
	b, _, _, prefix := ownerBroker(t)
	sub := messaging.Subscription{Name: "prompt", AckWait: 5 * time.Second}
	type delivery struct {
		attempt int
		at      time.Time
	}
	got := make(chan delivery, 2)
	member(t, b, sub, nil, func(ctx context.Context, _ event.Event) error {
		a := messaging.Attempt(ctx)
		select {
		case got <- delivery{a, time.Now()}:
		default:
		}
		if a == 1 {
			return errors.New("not yet")
		}
		return nil
	})
	publish(t, b, event.Event{ID: "n", Source: "/test", Type: prefix + ".t"})
	var ds []delivery
	for len(ds) < 2 {
		select {
		case d := <-got:
			ds = append(ds, d)
		case <-time.After(failsafe):
			t.Fatalf("deliveries %v, want two", ds)
		}
	}
	if ds[0].attempt != 1 || ds[1].attempt != 2 {
		t.Errorf("attempts %d then %d, want 1 then 2", ds[0].attempt, ds[1].attempt)
	}
	// Unsettled, the event would be back only after the consumer's AckWait,
	// over five seconds.
	if gap := ds[1].at.Sub(ds[0].at); gap > time.Second {
		t.Errorf("redelivered after %v, want well under the AckWait of %v", gap, sub.AckWait+nats.AckMargin)
	}
}

// A member handles one message at a time: while its handler holds one, the
// next waits on the server, unpulled, and is delivered once the first
// settles.
func TestSourceOneMessageAtATime(t *testing.T) {
	b, url, stream, prefix := ownerBroker(t)
	handling := make(chan string, 2)
	release := make(chan struct{})
	member(t, b, messaging.Subscription{Name: "serial"}, nil, func(ctx context.Context, e event.Event) error {
		handling <- e.ID
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})
	// Registered after the member's, so it runs first and frees a held
	// handler before the member shuts down.
	t.Cleanup(func() { close(release) })
	publish(t, b, event.Event{ID: "one", Source: "/test", Type: prefix + ".t"})
	publish(t, b, event.Event{ID: "two", Source: "/test", Type: prefix + ".t"})
	next := func(what string) string {
		t.Helper()
		select {
		case id := <-handling:
			return id
		case <-time.After(failsafe):
			t.Fatalf("timed out waiting for %s", what)
			return ""
		}
	}
	if id := next("the first event"); id != "one" {
		t.Fatalf("handled %s first, want one", id)
	}
	select {
	case id := <-handling:
		t.Fatalf("handled %s while the first was held", id)
	case <-time.After(500 * time.Millisecond):
	}
	if n := consumerInfo(t, jetStream(t, url), stream, "serial").NumAckPending; n != 1 {
		t.Errorf("ack pending %d while the handler holds one, want 1", n)
	}
	release <- struct{}{}
	if id := next("the second event"); id != "two" {
		t.Errorf("handled %s second, want two", id)
	}
}

// A source with no report function discards what it would report: it
// survives a message that does not decode and handles the next.
func TestSourceNilReportDiscards(t *testing.T) {
	b, url, _, prefix := ownerBroker(t)
	got := make(chan string, 1)
	member(t, b, messaging.Subscription{Name: "quiet"}, nil, func(_ context.Context, e event.Event) error {
		got <- e.ID
		return nil
	})
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	if _, err := jetStream(t, url).Publish(ctx, prefix+".t", []byte("not an event")); err != nil {
		t.Fatalf("publish raw bytes: %v", err)
	}
	publish(t, b, event.Event{ID: "after", Source: "/test", Type: prefix + ".t"})
	select {
	case id := <-got:
		if id != "after" {
			t.Errorf("handled %s, want after", id)
		}
	case <-time.After(failsafe):
		t.Fatal("the event after the undecodable one was not handled")
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

// A subscription filters on its types as subjects: its member receives
// the events of those types, each with its type, and none of another type,
// even one that extends a filtered type.
func TestSourceFiltersOnTypes(t *testing.T) {
	b, _, _, prefix := ownerBroker(t)
	want, also := prefix+".orders.created", prefix+".orders.paid"
	got := make(chan event.Event, 10)
	member(t, b, messaging.Subscription{Name: "typed", Types: []string{want, also}}, nil, func(_ context.Context, e event.Event) error {
		got <- e
		return nil
	})
	publish(t, b, event.Event{ID: "skip", Source: "/test", Type: prefix + ".orders.cancelled"})
	publish(t, b, event.Event{ID: "a", Source: "/test", Type: want})
	publish(t, b, event.Event{ID: "longer", Source: "/test", Type: want + ".late"})
	publish(t, b, event.Event{ID: "b", Source: "/test", Type: also})
	publish(t, b, event.Event{ID: "end", Source: "/test", Type: want})

	var received []string
	for len(received) < 3 {
		select {
		case e := <-got:
			received = append(received, e.ID+" "+e.Type)
		case <-time.After(failsafe):
			t.Fatalf("received %q, then nothing", received)
		}
	}
	if w := []string{"a " + want, "b " + also, "end " + want}; !slices.Equal(received, w) {
		t.Errorf("received %q, want %q", received, w)
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
	publish(t, own, event.Event{ID: "shared", Source: "/test", Type: prefix + ".t"})
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

// forward relays the server at rawURL over loopback, so a test can sever
// the server and restore it without stopping the shared one, and returns
// the forwarder and the URL it serves.
func forward(t *testing.T, rawURL string) (*processtest.Forwarder, string) {
	t.Helper()
	u, err := neturl.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse %s: %v", rawURL, err)
	}
	f := processtest.Forward(t, u.Host)
	return f, "nats://" + f.Addr()
}

// While the server is unreachable, the source reports each pull that fails
// and reports not ready; once it is back, the reports stop, the source is
// ready again, and it delivers.
func TestSourceReportsOutage(t *testing.T) {
	url := natstest.Open(t)
	stream, prefix := natstest.Scratch(t, url)
	f, relayed := forward(t, url)
	b := started(t, nats.Config{URL: relayed, Stream: stream, Prefix: prefix, Owner: new(true)})
	var r reports
	got := make(chan string, 1)
	src, err := b.Subscribe(messaging.Subscription{Name: "outage"}, r.report)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	receive(t, src, func(_ context.Context, e event.Event) error {
		select {
		case got <- e.ID:
		default:
		}
		return nil
	})
	eventually(t, "the source to be ready", src.Ready)

	f.Sever()
	cut := time.Now()
	eventually(t, "the source to report not ready", func() bool { return !src.Ready() })
	eventually(t, "repeated reports during the outage", func() bool { return len(r.seen()) >= 2 })
	during := r.seen()
	t.Logf("%d reports in %v of outage, the last: %v", len(during), time.Since(cut).Round(time.Millisecond), during[len(during)-1])
	if src.Ready() {
		t.Error("the source reports ready while the server is unreachable")
	}

	f.Restore(t)
	eventually(t, "the source to be ready again", src.Ready)
	settled := len(r.seen())
	// Two pulls' time, a pull's bound and its retry wait each: a source
	// still failing would report in it.
	time.Sleep(2 * (time.Second + 100*time.Millisecond))
	if after := r.seen(); len(after) != settled {
		t.Errorf("reports went on after the source was ready again: %v", after[settled:])
	}
	if !src.Ready() {
		t.Error("the source is not ready after the server came back")
	}
	publish(t, b, event.Event{ID: "back", Source: "/test", Type: prefix + ".t"})
	select {
	case id := <-got:
		if id != "back" {
			t.Errorf("delivered %q, want back", id)
		}
	case <-time.After(failsafe):
		t.Fatal("no event delivered after the server came back")
	}
}
