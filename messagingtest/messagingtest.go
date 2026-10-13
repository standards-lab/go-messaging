package messagingtest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-core/reactor"
	"github.com/standards-lab/go-messaging"
)

// failsafe bounds every wait for something that should happen, so a broken
// provider fails the case instead of hanging it.
const failsafe = 5 * time.Second

// quiet is how long a case watches for something that must not happen.
const quiet = 250 * time.Millisecond

// Run runs the conformance suite, each case as a subtest on a broker from
// newBroker.
func Run(t *testing.T, newBroker func(t *testing.T) messaging.Broker) {
	cases := []struct {
		name string
		fn   func(*testing.T, messaging.Broker)
	}{
		{"RoundTrip", testRoundTrip},
		{"TypeFilter", testTypeFilter},
		{"SingleTypeFilter", testSingleTypeFilter},
		{"StartsAtStreamBeginning", testStartsAtStreamBeginning},
		{"StartNewSkipsEarlier", testStartNewSkipsEarlier},
		{"NamesEachReceiveAll", testNamesEachReceiveAll},
		{"GroupSplitsWork", testGroupSplitsWork},
		{"ErrorRedelivers", testErrorRedelivers},
		{"PermanentTerminates", testPermanentTerminates},
		{"MaxDeliverBounds", testMaxDeliverBounds},
		{"MaxDeliverBoundsExpiry", testMaxDeliverBoundsExpiry},
		{"AckWaitRedelivers", testAckWaitRedelivers},
		{"DurableResumes", testDurableResumes},
		{"DrainKeepsAck", testDrainKeepsAck},
		{"Ready", testReady},
		{"SubscribeRejectsInvalid", testSubscribeRejectsInvalid},
		{"BindingMustMatch", testBindingMustMatch},
		{"BindingNormalizesTypes", testBindingNormalizesTypes},
		{"PublishRejectsInvalid", testPublishRejectsInvalid},
		{"TypeRule", testTypeRule},
		{"Deduplicates", testDeduplicates},
		{"AttemptStartsAtOne", testAttemptStartsAtOne},
		{"AttemptCountsAfterError", testAttemptCountsAfterError},
		{"AttemptCountsAfterExpiry", testAttemptCountsAfterExpiry},
		{"AttemptReachesMaxDeliver", testAttemptReachesMaxDeliver},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { c.fn(t, newBroker(t)) })
	}
}

func ev(id, typ string) event.Event {
	return event.Event{ID: id, Source: "/messagingtest", Type: typ}
}

func publish(t *testing.T, b messaging.Broker, es ...event.Event) {
	t.Helper()
	for _, e := range es {
		if err := b.Publish(context.Background(), e); err != nil {
			t.Fatalf("Publish %s: %v", e.ID, err)
		}
	}
}

// member subscribes under sub, starts a reactor on the source, and waits
// until it is receiving. The cleanup shuts it down if the case has not.
func member(t *testing.T, b messaging.Broker, sub messaging.Subscription, fn reactor.Func[event.Event]) *reactor.Reactor[event.Event] {
	t.Helper()
	src, err := b.Subscribe(sub, nil)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	r := reactor.New(src, fn)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = stop(r) })
	eventually(t, r.Ready, "the member to be receiving")
	return r
}

func stop(r *reactor.Reactor[event.Event]) error {
	ctx, cancel := context.WithTimeout(context.Background(), failsafe)
	defer cancel()
	return r.Shutdown(ctx)
}

func eventually(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(failsafe)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// deliveries records what each handler received.
type deliveries struct {
	mu  sync.Mutex
	ids []string
	by  map[string]string // id to the member that handled it last
}

func (d *deliveries) add(who string, e event.Event) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ids = append(d.ids, e.ID)
	if d.by == nil {
		d.by = map[string]string{}
	}
	d.by[e.ID] = who
}

func (d *deliveries) seen() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.ids...)
}

func (d *deliveries) count(id string) int {
	n := 0
	for _, got := range d.seen() {
		if got == id {
			n++
		}
	}
	return n
}

func (d *deliveries) has(id string) func() bool {
	return func() bool { return d.count(id) > 0 }
}

// record returns a handler that records each event and acknowledges it.
func (d *deliveries) record(who string) reactor.Func[event.Event] {
	return func(_ context.Context, e event.Event) error {
		d.add(who, e)
		return nil
	}
}

func testRoundTrip(t *testing.T, b messaging.Broker) {
	want := event.Event{
		ID:              "rt-1",
		Source:          "/grants",
		Type:            "lab.grant.approved",
		Subject:         "grant/42",
		DataContentType: "application/json",
		DataSchema:      "https://example.com/schemas/grant.json",
		Time:            time.Date(2026, 9, 24, 13, 0, 0, 123456789, time.UTC),
		Data:            []byte(`{"id":42}`),
		Extensions:      map[string]string{"traceparent": "00-abc-def-01"},
	}
	got := make(chan event.Event, 1)
	member(t, b, messaging.Subscription{Name: "rt"}, func(_ context.Context, e event.Event) error {
		got <- e
		return nil
	})
	publish(t, b, want)
	select {
	case e := <-got:
		if !reflect.DeepEqual(e, want) {
			t.Errorf("round trip:\n got %+v\nwant %+v", e, want)
		}
	case <-time.After(failsafe):
		t.Fatal("timed out waiting for the event")
	}
}

func testTypeFilter(t *testing.T, b messaging.Broker) {
	var d deliveries
	member(t, b, messaging.Subscription{Name: "filtered", Types: []string{"want", "also"}}, d.record("m"))
	publish(t, b, ev("a", "skip"), ev("b", "want"), ev("c", "also"), ev("d", "skip"), ev("end", "want"))
	eventually(t, d.has("end"), "the last matching event")
	if got, want := d.seen(), []string{"b", "c", "end"}; !reflect.DeepEqual(got, want) {
		t.Errorf("delivered %v, want %v", got, want)
	}
}

// A filter of one type, the form a provider may configure differently from
// several, admits that type alone.
func testSingleTypeFilter(t *testing.T, b messaging.Broker) {
	var d deliveries
	member(t, b, messaging.Subscription{Name: "single", Types: []string{"lab.want"}}, d.record("m"))
	publish(t, b, ev("a", "lab.skip"), ev("b", "lab.want"), ev("c", "lab.want.more"), ev("end", "lab.want"))
	eventually(t, d.has("end"), "the last matching event")
	if got, want := d.seen(), []string{"b", "end"}; !reflect.DeepEqual(got, want) {
		t.Errorf("delivered %v, want %v", got, want)
	}
}

// A new Name under StartAll, the zero value, starts at the beginning of the
// stream, so events published before anything subscribed still reach it.
func testStartsAtStreamBeginning(t *testing.T, b messaging.Broker) {
	publish(t, b, ev("early-1", "t"), ev("early-2", "t"))
	var d deliveries
	member(t, b, messaging.Subscription{Name: "late"}, d.record("m"))
	publish(t, b, ev("after", "t"))
	eventually(t, d.has("after"), "the event published after subscribing")
	if got, want := d.seen(), []string{"early-1", "early-2", "after"}; !reflect.DeepEqual(got, want) {
		t.Errorf("delivered %v, want %v", got, want)
	}
}

// A new Name under StartNew receives only the events published after its
// consumer is created. A provider may create it as late as the source's
// first Receive, so the case publishes the later events only once the member
// is ready, which is after that Receive has created the consumer.
func testStartNewSkipsEarlier(t *testing.T, b messaging.Broker) {
	publish(t, b, ev("early-1", "t"), ev("early-2", "t"))
	var d deliveries
	member(t, b, messaging.Subscription{Name: "fresh", Start: messaging.StartNew}, d.record("m"))
	publish(t, b, ev("after-1", "t"), ev("after-2", "t"))
	eventually(t, d.has("after-2"), "the events published after subscribing")
	time.Sleep(quiet)
	if got, want := d.seen(), []string{"after-1", "after-2"}; !reflect.DeepEqual(got, want) {
		t.Errorf("delivered %v, want %v", got, want)
	}
}

func testNamesEachReceiveAll(t *testing.T, b messaging.Broker) {
	var x, y deliveries
	member(t, b, messaging.Subscription{Name: "x"}, x.record("x"))
	member(t, b, messaging.Subscription{Name: "y"}, y.record("y"))
	want := []string{"1", "2", "3", "4", "5"}
	for _, id := range want {
		publish(t, b, ev(id, "t"))
	}
	eventually(t, func() bool { return len(x.seen()) == len(want) && len(y.seen()) == len(want) }, "both names to receive every event")
	if !reflect.DeepEqual(x.seen(), want) || !reflect.DeepEqual(y.seen(), want) {
		t.Errorf("x got %v, y got %v, want each %v", x.seen(), y.seen(), want)
	}
}

func testGroupSplitsWork(t *testing.T, b messaging.Broker) {
	var d deliveries
	slow := func(who string) reactor.Func[event.Event] {
		return func(_ context.Context, e event.Event) error {
			time.Sleep(10 * time.Millisecond)
			d.add(who, e)
			return nil
		}
	}
	sub := messaging.Subscription{Name: "workers"}
	member(t, b, sub, slow("a"))
	member(t, b, sub, slow("b"))
	const n = 20
	for i := range n {
		publish(t, b, ev(fmt.Sprint(i), "t"))
	}
	eventually(t, func() bool { return len(d.seen()) >= n }, "the group to handle every event")
	perMember := map[string]int{}
	for i := range n {
		id := fmt.Sprint(i)
		if c := d.count(id); c != 1 {
			t.Errorf("event %s handled %d times, want once", id, c)
		}
		d.mu.Lock()
		perMember[d.by[id]]++
		d.mu.Unlock()
	}
	if perMember["a"] == 0 || perMember["b"] == 0 {
		t.Errorf("work per member = %v, want both members to share it", perMember)
	}
}

func testErrorRedelivers(t *testing.T, b messaging.Broker) {
	const delay = 100 * time.Millisecond
	var mu sync.Mutex
	var times []time.Time
	member(t, b, messaging.Subscription{Name: "retry", RetryDelay: delay}, func(_ context.Context, _ event.Event) error {
		mu.Lock()
		defer mu.Unlock()
		times = append(times, time.Now())
		if len(times) == 1 {
			return errors.New("transient")
		}
		return nil
	})
	publish(t, b, ev("r", "t"))
	eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(times) >= 2 }, "the redelivery")
	mu.Lock()
	gap := times[1].Sub(times[0])
	mu.Unlock()
	if gap < delay {
		t.Errorf("redelivered after %v, want no sooner than %v", gap, delay)
	}
	time.Sleep(quiet)
	mu.Lock()
	defer mu.Unlock()
	if len(times) != 2 {
		t.Errorf("handled %d times, want 2: an acknowledged redelivery is final", len(times))
	}
}

func testPermanentTerminates(t *testing.T, b messaging.Broker) {
	var d deliveries
	member(t, b, messaging.Subscription{Name: "perm", RetryDelay: 10 * time.Millisecond}, func(ctx context.Context, e event.Event) error {
		d.add("m", e)
		if e.ID == "p" {
			return fmt.Errorf("handle: %w", event.Permanent(errors.New("malformed")))
		}
		return nil
	})
	publish(t, b, ev("p", "t"), ev("end", "t"))
	eventually(t, d.has("end"), "the event after the terminated one")
	time.Sleep(quiet)
	if c := d.count("p"); c != 1 {
		t.Errorf("the terminated event was handled %d times, want once", c)
	}
}

func testMaxDeliverBounds(t *testing.T, b messaging.Broker) {
	var d deliveries
	member(t, b, messaging.Subscription{Name: "bounded", MaxDeliver: 3, RetryDelay: 10 * time.Millisecond}, func(_ context.Context, e event.Event) error {
		d.add("m", e)
		return errors.New("always")
	})
	publish(t, b, ev("m", "t"))
	eventually(t, func() bool { return d.count("m") >= 3 }, "three deliveries")
	time.Sleep(quiet)
	if c := d.count("m"); c != 3 {
		t.Errorf("delivered %d times, want MaxDeliver 3", c)
	}
}

// Deliveries that expire count toward MaxDeliver like those that fail.
func testMaxDeliverBoundsExpiry(t *testing.T, b messaging.Broker) {
	var d deliveries
	member(t, b, messaging.Subscription{Name: "overrun", MaxDeliver: 2, AckWait: 50 * time.Millisecond}, func(ctx context.Context, e event.Event) error {
		d.add("m", e)
		<-ctx.Done()
		return nil // always late, so ignored
	})
	publish(t, b, ev("o", "t"))
	eventually(t, func() bool { return d.count("o") >= 2 }, "two deliveries")
	time.Sleep(quiet)
	if c := d.count("o"); c != 2 {
		t.Errorf("delivered %d times, want MaxDeliver 2", c)
	}
}

func testAckWaitRedelivers(t *testing.T, b messaging.Broker) {
	const wait = 100 * time.Millisecond
	type attempt struct {
		hasDeadline bool
		err         error
	}
	var mu sync.Mutex
	var attempts []attempt
	member(t, b, messaging.Subscription{Name: "slow", AckWait: wait, RetryDelay: 10 * time.Millisecond}, func(ctx context.Context, _ event.Event) error {
		mu.Lock()
		n := len(attempts)
		attempts = append(attempts, attempt{})
		mu.Unlock()
		_, ok := ctx.Deadline()
		var err error
		if n == 0 {
			select {
			case <-ctx.Done():
				err = ctx.Err()
			case <-time.After(failsafe):
			}
		}
		mu.Lock()
		attempts[n] = attempt{ok, err}
		mu.Unlock()
		return nil // late on the first attempt, so ignored
	})
	publish(t, b, ev("s", "t"))
	eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(attempts) >= 2 }, "the redelivery after AckWait")
	mu.Lock()
	first := attempts[0]
	mu.Unlock()
	if !first.hasDeadline {
		t.Error("the handler's context carries no AckWait deadline")
	}
	if !errors.Is(first.err, context.DeadlineExceeded) {
		t.Errorf("the first attempt's context ended with %v, want the AckWait deadline", first.err)
	}
	time.Sleep(quiet)
	mu.Lock()
	defer mu.Unlock()
	if len(attempts) != 2 {
		t.Errorf("handled %d times, want 2", len(attempts))
	}
}

// A durable resumes where it left off under either start position: the
// start position places only a new consumer, so a StartNew durable still
// receives what was published while it had no member.
func testDurableResumes(t *testing.T, b messaging.Broker) {
	for _, c := range []struct {
		name  string
		start messaging.Start
	}{{"all", messaging.StartAll}, {"new", messaging.StartNew}} {
		name := c.name
		sub := messaging.Subscription{Name: "durable-" + name, Start: c.start}
		e := func(n int) event.Event { return ev(fmt.Sprintf("%s-e%d", name, n), "t") }
		var first deliveries
		m1 := member(t, b, sub, first.record("1"))
		publish(t, b, e(1))
		eventually(t, first.has(e(1).ID), "the first member to handle e1")
		if err := stop(m1); err != nil {
			t.Fatalf("%s: Shutdown: %v", name, err)
		}
		publish(t, b, e(2), e(3))
		var second deliveries
		member(t, b, sub, second.record("2"))
		eventually(t, second.has(e(3).ID), "the second member to catch up")
		if got, want := second.seen(), []string{e(2).ID, e(3).ID}; !reflect.DeepEqual(got, want) {
			t.Errorf("%s: the resumed member got %v, want %v", name, got, want)
		}
	}
}

// The subscription's AckWait is short, so a lost acknowledgement would
// redeliver the event within the case's watch, which runs to twice AckWait
// past the handler's start, leaving a provider room for its own margin.
func testDrainKeepsAck(t *testing.T, b messaging.Broker) {
	const ackWait = 500 * time.Millisecond
	sub := messaging.Subscription{Name: "drain", AckWait: ackWait, RetryDelay: 10 * time.Millisecond}
	started := make(chan struct{})
	var startedAt time.Time
	var finished bool
	m1 := member(t, b, sub, func(ctx context.Context, _ event.Event) error {
		startedAt = time.Now()
		close(started)
		time.Sleep(100 * time.Millisecond)
		finished = ctx.Err() == nil
		return nil
	})
	publish(t, b, ev("in-flight", "t"))
	select {
	case <-started:
	case <-time.After(failsafe):
		t.Fatal("timed out waiting for the handler")
	}
	if err := stop(m1); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if !finished {
		t.Fatal("the drain interrupted the handler")
	}
	var d deliveries
	member(t, b, sub, d.record("2"))
	publish(t, b, ev("end", "t"))
	eventually(t, d.has("end"), "the next member to receive new events")
	time.Sleep(time.Until(startedAt.Add(2 * ackWait)))
	if c := d.count("in-flight"); c != 0 {
		t.Errorf("the drained event was redelivered %d times; its acknowledgement was lost", c)
	}
}

func testReady(t *testing.T, b messaging.Broker) {
	src, err := b.Subscribe(messaging.Subscription{Name: "ready"}, nil)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if src.Ready() {
		t.Error("ready before receiving")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- src.Receive(ctx, func(context.Context, event.Event) error { return nil })
	}()
	eventually(t, src.Ready, "the source to be ready")
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Receive: %v", err)
		}
	case <-time.After(failsafe):
		t.Fatal("Receive did not return after its context ended")
	}
	if src.Ready() {
		t.Error("ready after Receive returned")
	}
}

func testSubscribeRejectsInvalid(t *testing.T, b messaging.Broker) {
	if _, err := b.Subscribe(messaging.Subscription{}, nil); err == nil {
		t.Error("Subscribe accepted a subscription with no name")
	}
	for _, name := range []string{"a/b", "a\\b"} {
		if _, err := b.Subscribe(messaging.Subscription{Name: name}, nil); err == nil {
			t.Errorf("Subscribe accepted the name %q, which no durable can take", name)
		}
	}
}

func testPublishRejectsInvalid(t *testing.T, b messaging.Broker) {
	if err := b.Publish(context.Background(), event.Event{ID: "no-source"}); err == nil {
		t.Error("Publish accepted an invalid event")
	}
}

// A later subscription under an existing Name binds its consumer: the same
// configuration binds, and a different one fails at Subscribe or at Receive.
func testBindingMustMatch(t *testing.T, b messaging.Broker) {
	sub := messaging.Subscription{Name: "bound", MaxDeliver: 3}
	var d deliveries
	member(t, b, sub, d.record("1"))
	member(t, b, sub, d.record("2"))

	maxDeliver := sub
	maxDeliver.MaxDeliver = 5
	start := sub
	start.Start = messaging.StartNew
	for what, other := range map[string]messaging.Subscription{"MaxDeliver": maxDeliver, "Start": start} {
		src, err := b.Subscribe(other, nil)
		if err != nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), failsafe)
		err = src.Receive(ctx, func(context.Context, event.Event) error { return nil })
		cancel()
		if err == nil {
			t.Errorf("a different %s under an existing Name bound the consumer", what)
		}
	}
}

// Subscriptions whose Types differ only in order and repeats mean the same
// filter, as [messaging.Subscription.Normalize] gives it, so they bind one
// consumer and share its work.
func testBindingNormalizesTypes(t *testing.T, b messaging.Broker) {
	var d deliveries
	first := member(t, b, messaging.Subscription{Name: "normal", Types: []string{"b", "a", "a"}}, d.record("1"))
	second := member(t, b, messaging.Subscription{Name: "normal", Types: []string{"a", "b"}}, d.record("2"))
	publish(t, b, ev("1", "a"), ev("2", "b"), ev("3", "c"), ev("end", "a"))
	eventually(t, func() bool { return d.count("1")+d.count("2")+d.count("end") == 3 }, "the group to handle every matching event")
	time.Sleep(quiet)
	if got := len(d.seen()); got != 3 {
		t.Errorf("delivered %v, want 1, 2, and end once each", d.seen())
	}
	if !first.Ready() || !second.Ready() {
		t.Error("a member stopped: the subscriptions did not bind one consumer")
	}
}

// A type that breaks [messaging.CheckType] is refused at both ends.
func testTypeRule(t *testing.T, b messaging.Broker) {
	for _, bad := range []string{"a..b", "a.*", "a.>", "a b"} {
		if _, err := b.Subscribe(messaging.Subscription{Name: "typed", Types: []string{bad}}, nil); err == nil {
			t.Errorf("Subscribe accepted type %q", bad)
		}
		if err := b.Publish(context.Background(), ev("bad", bad)); err == nil {
			t.Errorf("Publish accepted type %q", bad)
		}
	}
}

// A repeat of a source and id is accepted and dropped, whatever it carries;
// a new id, and the same id from another source, still deliver.
func testDeduplicates(t *testing.T, b messaging.Broker) {
	var d deliveries
	got := map[string][]byte{}
	var mu sync.Mutex
	member(t, b, messaging.Subscription{Name: "dedup"}, func(_ context.Context, e event.Event) error {
		mu.Lock()
		got[e.ID] = e.Data
		mu.Unlock()
		d.add("m", e)
		return nil
	})
	first, repeat, elsewhere := ev("once", "t"), ev("once", "t"), ev("once", "t")
	first.Data, repeat.Data = []byte("first"), []byte("repeat")
	elsewhere.Source, elsewhere.Data = "/elsewhere", []byte("first")
	publish(t, b, first, repeat, elsewhere, ev("end", "t"))
	eventually(t, d.has("end"), "the event after the repeat")
	time.Sleep(quiet)
	if c := d.count("once"); c != 2 {
		t.Errorf("the id was delivered %d times, want twice: once for each source", c)
	}
	mu.Lock()
	defer mu.Unlock()
	if string(got["once"]) != "first" {
		t.Errorf("delivered %q, want the first publish's data", got["once"])
	}
}

// attempts records the attempt each delivery's context carried, in order.
type attempts struct {
	mu sync.Mutex
	ns []int
}

func (a *attempts) add(ctx context.Context) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ns = append(a.ns, messaging.Attempt(ctx))
	return len(a.ns)
}

func (a *attempts) seen() []int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]int(nil), a.ns...)
}

func (a *attempts) atLeast(n int) func() bool {
	return func() bool { return len(a.seen()) >= n }
}

// The first delivery of an event is attempt 1, so an acknowledged event's
// only delivery carries 1, whatever came before it on the stream.
func testAttemptStartsAtOne(t *testing.T, b messaging.Broker) {
	var a attempts
	member(t, b, messaging.Subscription{Name: "first"}, func(ctx context.Context, _ event.Event) error {
		a.add(ctx)
		return nil
	})
	publish(t, b, ev("1", "t"), ev("2", "t"))
	eventually(t, a.atLeast(2), "both deliveries")
	if got, want := a.seen(), []int{1, 1}; !reflect.DeepEqual(got, want) {
		t.Errorf("attempts %v, want %v", got, want)
	}
}

// A redelivery after the handler's error is the next attempt.
func testAttemptCountsAfterError(t *testing.T, b messaging.Broker) {
	var a attempts
	member(t, b, messaging.Subscription{Name: "after-error", RetryDelay: 10 * time.Millisecond}, func(ctx context.Context, _ event.Event) error {
		if a.add(ctx) == 1 {
			return errors.New("transient")
		}
		return nil
	})
	publish(t, b, ev("e", "t"))
	eventually(t, a.atLeast(2), "the redelivery")
	time.Sleep(quiet)
	if got, want := a.seen(), []int{1, 2}; !reflect.DeepEqual(got, want) {
		t.Errorf("attempts %v, want %v", got, want)
	}
}

// A redelivery after the handler overran AckWait is the next attempt, as
// one after an error is.
func testAttemptCountsAfterExpiry(t *testing.T, b messaging.Broker) {
	var a attempts
	member(t, b, messaging.Subscription{Name: "after-expiry", AckWait: 100 * time.Millisecond}, func(ctx context.Context, _ event.Event) error {
		if a.add(ctx) == 1 {
			<-ctx.Done()
		}
		return nil // late on the first attempt, so ignored
	})
	publish(t, b, ev("x", "t"))
	eventually(t, a.atLeast(2), "the redelivery after AckWait")
	time.Sleep(quiet)
	if got, want := a.seen(), []int{1, 2}; !reflect.DeepEqual(got, want) {
		t.Errorf("attempts %v, want %v", got, want)
	}
}

// An event bounded by MaxDeliver is delivered on attempts 1 through
// MaxDeliver, so a handler on attempt MaxDeliver knows it is the last.
func testAttemptReachesMaxDeliver(t *testing.T, b messaging.Broker) {
	const maxDeliver = 3
	var a attempts
	member(t, b, messaging.Subscription{Name: "last", MaxDeliver: maxDeliver, RetryDelay: 10 * time.Millisecond}, func(ctx context.Context, _ event.Event) error {
		a.add(ctx)
		return errors.New("always")
	})
	publish(t, b, ev("m", "t"))
	eventually(t, a.atLeast(maxDeliver), "MaxDeliver deliveries")
	time.Sleep(quiet)
	if got, want := a.seen(), []int{1, 2, maxDeliver}; !reflect.DeepEqual(got, want) {
		t.Errorf("attempts %v, want %v", got, want)
	}
}
