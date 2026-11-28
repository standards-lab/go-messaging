package messagingtest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-core/reactor"
	"github.com/standards-lab/go-messaging"
)

// QueueHarness is a queue under test, as a provider hands it to
// [RunQueue], with the time it runs on.
type QueueHarness struct {
	// Queue is an empty queue.
	Queue messaging.Queue
	// Unit is the duration the cases build each AckWait and Backoff from:
	// long enough that the provider delivers and settles an event well
	// within it.
	Unit time.Duration
	// Advance lets d of the queue's time pass: a provider on an injected
	// clock moves the clock, and a real-time provider waits d.
	Advance func(d time.Duration)
	// DeadLetter is a dead-letter subject the queue accepts, with the
	// subject one token below it, DeadLetter+".dead", for its own dead
	// letters.
	DeadLetter string
	// Runtime is a runtime whose [messaging.Runtime.Work] builds the
	// reactor cases' workers. Its drain timeout must be long enough that a
	// handler ending at the drain unwinds within half of it.
	Runtime *messaging.Runtime
	// SkipDeadLetter skips the cases that dead-letter, for a provider that
	// does not dead-letter yet.
	SkipDeadLetter bool
}

// RunQueue runs the work queue's conformance cases, each as a subtest on a
// harness from newQueue.
func RunQueue(t *testing.T, newQueue func(t *testing.T) QueueHarness) {
	cases := []struct {
		name string
		fn   func(*testing.T, QueueHarness)
		dead bool
	}{
		{"EnqueueHolds", testEnqueueHolds, false},
		{"EnqueueRejectsInvalid", testEnqueueRejectsInvalid, false},
		{"EnqueueDeduplicates", testEnqueueDeduplicates, false},
		{"WorkersShareQueue", testWorkersShareQueue, false},
		{"ExtensionKeepsClaim", testExtensionKeepsClaim, false},
		{"AckRemoves", testAckRemoves, false},
		{"ErrorBacksOff", testErrorBacksOff, false},
		{"PermanentDeadLetters", testPermanentDeadLetters, true},
		{"FinalAttemptDeadLetters", testFinalAttemptDeadLetters, true},
		{"UnsettledFinalDeadLetters", testUnsettledFinalDeadLetters, true},
		{"DeadLetterWorkerAcks", testDeadLetterWorkerAcks, true},
		{"SubscribeRejectsInvalidWorker", testSubscribeRejectsInvalidWorker, false},
		{"DrainKeepsNil", testDrainKeepsNil, false},
		{"DrainRedeliversError", testDrainRedeliversError, false},
		{"ReactorDrainEndsHandler", testReactorDrainEndsHandler, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newQueue(t)
			if c.dead && h.SkipDeadLetter {
				t.Skip("the provider does not dead-letter yet")
			}
			c.fn(t, h)
		})
	}
}

func jobWorker(h QueueHarness, name string, maxDeliver int) messaging.Worker {
	return messaging.Worker{Name: name, MaxDeliver: maxDeliver, DeadLetter: h.DeadLetter}
}

func deadWorker(h QueueHarness, name string) messaging.Worker {
	return messaging.Worker{Name: name, Subject: h.DeadLetter, MaxDeliver: 1, DeadLetter: h.DeadLetter + ".dead"}
}

func enqueue(t *testing.T, q messaging.Queue, es ...event.Event) {
	t.Helper()
	for _, e := range es {
		if err := q.Enqueue(context.Background(), e); err != nil {
			t.Fatalf("Enqueue %s: %v", e.ID, err)
		}
	}
}

type running struct {
	cancel context.CancelFunc
	done   chan error
}

func work(t *testing.T, q messaging.Queue, w messaging.Worker, fn reactor.Func[event.Event]) *running {
	t.Helper()
	src, err := q.Subscribe(w, nil)
	if err != nil {
		t.Fatalf("Subscribe %s: %v", w.Name, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- src.Receive(ctx, fn) }()
	t.Cleanup(func() { _ = r.stop() })
	eventually(t, src.Ready, "the worker to be receiving")
	return r
}

func (r *running) stop() error {
	r.cancel()
	select {
	case err := <-r.done:
		r.done <- err
		return err
	case <-time.After(failsafe):
		return errors.New("the source did not return from Receive after its context ended")
	}
}

func stopWorker(t *testing.T, r *running) {
	t.Helper()
	if err := r.stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

type call struct {
	event   event.Event
	attempt int
	final   bool
	dead    messaging.DeadLetter
	isDead  bool
}

type calls struct {
	mu   sync.Mutex
	list []call
}

func (c *calls) add(ctx context.Context, w messaging.Worker, e event.Event) {
	d, ok := messaging.DeadLetterOf(ctx)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.list = append(c.list, call{event: e, attempt: messaging.Attempt(ctx), final: w.Final(ctx), dead: d, isDead: ok})
}

func (c *calls) of(id string) []call {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []call
	for _, v := range c.list {
		if v.event.ID == id {
			out = append(out, v)
		}
	}
	return out
}

func (c *calls) count(id string) int { return len(c.of(id)) }

func (c *calls) has(id string) func() bool {
	return func() bool { return c.count(id) > 0 }
}

func (c *calls) reach(id string, n int) func() bool {
	return func() bool { return c.count(id) >= n }
}

func (c *calls) handler(w messaging.Worker, fail func(event.Event) error) reactor.Func[event.Event] {
	return func(ctx context.Context, e event.Event) error {
		c.add(ctx, w, e)
		if fail != nil {
			return fail(e)
		}
		return nil
	}
}

func failing(id string, err error) func(event.Event) error {
	return func(e event.Event) error {
		if e.ID == id {
			return err
		}
		return nil
	}
}

func flush(t *testing.T, q messaging.Queue, c *calls, id string) {
	t.Helper()
	enqueue(t, q, ev(id, "t"))
	eventually(t, c.has(id), "the worker to handle "+id)
}

func recvCtx(t *testing.T, ch <-chan context.Context, what string) context.Context {
	t.Helper()
	select {
	case ctx := <-ch:
		return ctx
	case <-time.After(failsafe):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

func testEnqueueHolds(t *testing.T, h QueueHarness) {
	enqueue(t, h.Queue, ev("held", "t"))
	var c calls
	w := jobWorker(h, "holds", 3)
	work(t, h.Queue, w, c.handler(w, nil))
	eventually(t, c.has("held"), "the event enqueued before any worker")
}

func testEnqueueRejectsInvalid(t *testing.T, h QueueHarness) {
	if err := h.Queue.Enqueue(context.Background(), event.Event{ID: "no-source"}); err == nil {
		t.Error("Enqueue accepted an invalid event")
	}
	for _, bad := range []string{"a..b", "a.*", "a.>", "a b"} {
		if err := h.Queue.Enqueue(context.Background(), ev("bad", bad)); err == nil {
			t.Errorf("Enqueue accepted type %q", bad)
		}
	}
}

func testEnqueueDeduplicates(t *testing.T, h QueueHarness) {
	first, repeat, elsewhere := ev("once", "t"), ev("once", "t"), ev("once", "t")
	first.Data, repeat.Data = []byte("first"), []byte("repeat")
	elsewhere.Source, elsewhere.Data = "/elsewhere", []byte("elsewhere")
	enqueue(t, h.Queue, first, repeat, elsewhere, ev("end", "t"))
	var c calls
	w := jobWorker(h, "dedup", 3)
	work(t, h.Queue, w, c.handler(w, nil))
	eventually(t, c.has("end"), "the event after the repeat")
	var got []string
	for _, v := range c.of("once") {
		got = append(got, string(v.event.Data))
	}
	if want := []string{"first", "elsewhere"}; !reflect.DeepEqual(got, want) {
		t.Errorf("handled %v, want %v: the repeat is stored once, another source's id is its own", got, want)
	}
}

func testWorkersShareQueue(t *testing.T, h QueueHarness) {
	const n = 6
	w := jobWorker(h, "share", 3)
	type start struct{ who, id string }
	starts := make(chan start, n)
	release := make(chan struct{})
	var mu sync.Mutex
	inFlight := map[string]int{}
	handled := map[string][]string{}
	var overlap []string
	handler := func(who string) reactor.Func[event.Event] {
		return func(_ context.Context, e event.Event) error {
			mu.Lock()
			inFlight[who]++
			if inFlight[who] > 1 {
				overlap = append(overlap, who)
			}
			handled[e.ID] = append(handled[e.ID], who)
			mu.Unlock()
			starts <- start{who, e.ID}
			<-release
			mu.Lock()
			inFlight[who]--
			mu.Unlock()
			return nil
		}
	}
	work(t, h.Queue, w, handler("a"))
	work(t, h.Queue, w, handler("b"))
	for i := range n {
		enqueue(t, h.Queue, ev(fmt.Sprint(i), "t"))
	}
	busy := map[string]bool{}
	for range 2 {
		select {
		case s := <-starts:
			busy[s.who] = true
		case <-time.After(failsafe):
			t.Fatal("timed out waiting for both workers to take an event")
		}
	}
	if !busy["a"] || !busy["b"] {
		t.Errorf("the first two events went to %v, want one to each worker", busy)
	}
	for range n - 2 {
		release <- struct{}{}
		select {
		case <-starts:
		case <-time.After(failsafe):
			t.Fatal("timed out waiting for the next event")
		}
	}
	close(release)
	mu.Lock()
	defer mu.Unlock()
	if len(overlap) > 0 {
		t.Errorf("workers %v held two events at once", overlap)
	}
	for i := range n {
		if got := handled[fmt.Sprint(i)]; len(got) != 1 {
			t.Errorf("event %d handled by %v, want exactly one worker", i, got)
		}
	}
}

func testExtensionKeepsClaim(t *testing.T, h QueueHarness) {
	w := jobWorker(h, "extend", 3)
	w.AckWait = 2 * h.Unit
	var c calls
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	handler := func(ctx context.Context, e event.Event) error {
		c.add(ctx, w, e)
		if e.ID == "long" {
			started <- ctx
			<-release
		}
		return nil
	}
	work(t, h.Queue, w, handler)
	work(t, h.Queue, w, handler)
	enqueue(t, h.Queue, ev("long", "t"))
	hctx := recvCtx(t, started, "the long handler")
	h.Advance(4 * w.AckWait)
	if err := hctx.Err(); err != nil {
		t.Errorf("the handler's context ended with %v while its delivery was extended", err)
	}
	enqueue(t, h.Queue, ev("end", "t"))
	close(release)
	eventually(t, c.has("end"), "the event after the long one")
	if n := c.count("long"); n != 1 {
		t.Errorf("the long job was delivered %d times, want once: the idle worker took it while its claim was extended", n)
	}
}

func testAckRemoves(t *testing.T, h QueueHarness) {
	w := jobWorker(h, "ack", 3)
	w.AckWait = 2 * h.Unit
	var c calls
	work(t, h.Queue, w, c.handler(w, nil))
	enqueue(t, h.Queue, ev("done", "t"))
	flush(t, h.Queue, &c, "settled")
	h.Advance(2 * w.AckWait)
	flush(t, h.Queue, &c, "end")
	if n := c.count("done"); n != 1 {
		t.Errorf("the acknowledged job was delivered %d times, want once", n)
	}
}

func testErrorBacksOff(t *testing.T, h QueueHarness) {
	u := h.Unit
	w := jobWorker(h, "backoff", 3)
	w.AckWait = 40 * u
	w.Backoff = []time.Duration{4 * u, 8 * u}
	var c calls
	work(t, h.Queue, w, c.handler(w, func(e event.Event) error {
		if e.ID == "x" && c.count("x") < 3 {
			return errors.New("transient")
		}
		return nil
	}))
	enqueue(t, h.Queue, ev("x", "t"))
	flush(t, h.Queue, &c, "s1")

	h.Advance(2 * u)
	flush(t, h.Queue, &c, "s2")
	if n := c.count("x"); n != 1 {
		t.Fatalf("delivered %d times halfway through the first backoff, want once", n)
	}
	h.Advance(2 * u)
	eventually(t, c.reach("x", 2), "the redelivery after the first backoff")
	flush(t, h.Queue, &c, "s3")

	h.Advance(6 * u)
	flush(t, h.Queue, &c, "s4")
	if n := c.count("x"); n != 2 {
		t.Fatalf("delivered %d times before the second backoff ended, want twice: attempt 2 waits Backoff[1]", n)
	}
	h.Advance(2 * u)
	eventually(t, c.reach("x", 3), "the redelivery after the second backoff")

	var got []int
	for _, v := range c.of("x") {
		got = append(got, v.attempt)
	}
	if want := []int{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Errorf("attempts %v, want %v", got, want)
	}
}

func testPermanentDeadLetters(t *testing.T, h QueueHarness) {
	want := event.Event{
		ID:              "perm-1",
		Source:          "/grants",
		Type:            "lab.grant.review",
		Subject:         "grant/42",
		DataContentType: "application/json",
		DataSchema:      "https://example.com/schemas/grant.json",
		Time:            time.Date(2026, 9, 24, 13, 0, 0, 123456789, time.UTC),
		Data:            []byte(`{"id":42}`),
		Extensions:      map[string]string{"traceparent": "00-abc-def-01"},
	}
	w := jobWorker(h, "perm", 3)
	var jobs, dead calls
	perm := fmt.Errorf("handle: %w", event.Permanent(errors.New("malformed")))
	work(t, h.Queue, w, jobs.handler(w, failing(want.ID, perm)))
	dw := deadWorker(h, "perm-dead")
	work(t, h.Queue, dw, dead.handler(dw, nil))
	enqueue(t, h.Queue, want)
	eventually(t, dead.has(want.ID), "the dead letter")

	got := dead.of(want.ID)[0]
	if !reflect.DeepEqual(got.event, want) {
		t.Errorf("dead letter:\n got %+v\nwant %+v", got.event, want)
	}
	if wantDead := (messaging.DeadLetter{Attempts: 1, Errored: true, Err: perm.Error()}); !got.isDead || got.dead != wantDead {
		t.Errorf("DeadLetterOf = %+v, %v; want %+v", got.dead, got.isDead, wantDead)
	}
	flush(t, h.Queue, &jobs, "end")
	if n := jobs.count(want.ID); n != 1 {
		t.Errorf("the permanently failed job was delivered %d times, want once", n)
	}
}

func testFinalAttemptDeadLetters(t *testing.T, h QueueHarness) {
	const maxDeliver = 3
	w := jobWorker(h, "final", maxDeliver)
	var jobs, dead calls
	work(t, h.Queue, w, jobs.handler(w, failing("x", errors.New("boom"))))
	dw := deadWorker(h, "final-dead")
	work(t, h.Queue, dw, dead.handler(dw, nil))
	enqueue(t, h.Queue, ev("x", "t"))
	eventually(t, dead.has("x"), "the dead letter")

	var attempts []int
	var finals []bool
	for _, v := range jobs.of("x") {
		attempts = append(attempts, v.attempt)
		finals = append(finals, v.final)
	}
	if want := []int{1, 2, maxDeliver}; !reflect.DeepEqual(attempts, want) {
		t.Errorf("attempts %v, want %v", attempts, want)
	}
	if want := []bool{false, false, true}; !reflect.DeepEqual(finals, want) {
		t.Errorf("Final %v, want %v", finals, want)
	}
	got := dead.of("x")[0]
	if want := (messaging.DeadLetter{Attempts: maxDeliver, Errored: true, Err: "boom"}); got.dead != want {
		t.Errorf("DeadLetterOf = %+v, want %+v", got.dead, want)
	}
	flush(t, h.Queue, &jobs, "end")
	if n := jobs.count("x"); n != maxDeliver {
		t.Errorf("delivered %d times, want MaxDeliver %d: the dead-lettered job left the queue", n, maxDeliver)
	}
}

func testUnsettledFinalDeadLetters(t *testing.T, h QueueHarness) {
	w := jobWorker(h, "unsettled", 1)
	w.AckWait = 40 * h.Unit
	started := make(chan context.Context, 1)
	first := work(t, h.Queue, w, func(ctx context.Context, _ event.Event) error {
		started <- ctx
		<-ctx.Done()
		return ctx.Err()
	})
	enqueue(t, h.Queue, ev("x", "t"))
	recvCtx(t, started, "the final attempt")
	stopWorker(t, first)

	var jobs, dead calls
	dw := deadWorker(h, "unsettled-dead")
	work(t, h.Queue, dw, dead.handler(dw, nil))
	work(t, h.Queue, w, jobs.handler(w, func(event.Event) error { return errors.New("ran") }))
	eventually(t, dead.has("x"), "the dead letter")
	if n := jobs.count("x"); n != 0 {
		t.Errorf("the handler ran %d times past the final attempt, want none", n)
	}
	got := dead.of("x")[0]
	if want := (messaging.DeadLetter{Attempts: 1}); got.dead != want {
		t.Errorf("DeadLetterOf = %+v, want %+v: the final attempt ended with no error", got.dead, want)
	}
}

func testDeadLetterWorkerAcks(t *testing.T, h QueueHarness) {
	w := jobWorker(h, "acked-off", 1)
	var jobs, first, second calls
	work(t, h.Queue, w, jobs.handler(w, func(event.Event) error { return errors.New("always") }))
	dw := deadWorker(h, "acked-off-dead")
	d1 := work(t, h.Queue, dw, first.handler(dw, nil))
	enqueue(t, h.Queue, ev("x", "t"))
	eventually(t, first.has("x"), "the dead letter")
	if !first.of("x")[0].isDead {
		t.Error("the dead-letter worker's context carries no DeadLetter")
	}
	stopWorker(t, d1)

	work(t, h.Queue, dw, second.handler(dw, nil))
	enqueue(t, h.Queue, ev("y", "t"))
	eventually(t, second.has("y"), "the next dead letter")
	if n := second.count("x"); n != 0 {
		t.Errorf("the acknowledged dead letter was delivered again %d times", n)
	}
}

func testSubscribeRejectsInvalidWorker(t *testing.T, h QueueHarness) {
	_, err := h.Queue.Subscribe(messaging.Worker{Name: "a/b", Subject: "a..b", Backoff: []time.Duration{-1}}, nil)
	if err == nil {
		t.Fatal("Subscribe accepted an invalid worker")
	}
	for _, problem := range []string{"name", "subject", "max deliver", "backoff", "dead letter"} {
		if !strings.Contains(err.Error(), problem) {
			t.Errorf("Subscribe's error %q does not report the %s", err, problem)
		}
	}
}

func testDrainKeepsNil(t *testing.T, h QueueHarness) {
	w := jobWorker(h, "drain-nil", 3)
	w.AckWait = 2 * h.Unit
	started := make(chan context.Context, 1)
	first := work(t, h.Queue, w, func(ctx context.Context, _ event.Event) error {
		started <- ctx
		<-ctx.Done()
		return nil
	})
	enqueue(t, h.Queue, ev("x", "t"))
	recvCtx(t, started, "the handler")
	stopWorker(t, first)

	h.Advance(2 * w.AckWait)
	var c calls
	work(t, h.Queue, w, c.handler(w, nil))
	flush(t, h.Queue, &c, "end")
	if n := c.count("x"); n != 0 {
		t.Errorf("the drained job was redelivered %d times: its acknowledgement was lost", n)
	}
}

func testDrainRedeliversError(t *testing.T, h QueueHarness) {
	w := jobWorker(h, "drain-error", 3)
	w.AckWait = 40 * h.Unit
	started := make(chan context.Context, 1)
	first := work(t, h.Queue, w, func(ctx context.Context, _ event.Event) error {
		started <- ctx
		<-ctx.Done()
		return ctx.Err()
	})
	enqueue(t, h.Queue, ev("x", "t"))
	recvCtx(t, started, "the handler")
	stopWorker(t, first)

	var c calls
	work(t, h.Queue, w, c.handler(w, nil))
	eventually(t, c.has("x"), "the drained job's redelivery")
	if got := c.of("x")[0].attempt; got != 2 {
		t.Errorf("redelivered as attempt %d, want 2", got)
	}
}

func testReactorDrainEndsHandler(t *testing.T, h QueueHarness) {
	w := jobWorker(h, "reactor-drain", 3)
	w.AckWait = 40 * h.Unit
	type job struct{ N int }
	started := make(chan context.Context, 1)
	got := make(chan job, 1)
	r, err := h.Runtime.Work(h.Queue, w, func(ctx context.Context, data job) error {
		got <- data
		started <- ctx
		<-ctx.Done()
		return ctx.Err()
	})
	if err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = r.Shutdown(context.Background())
		}
	})
	e := ev("x", "t")
	e.Data = []byte(`{"N":7}`)
	enqueue(t, h.Queue, e)
	recvCtx(t, started, "the handler")
	if data := <-got; data.N != 7 {
		t.Errorf("the handler was given %+v, want the job's data decoded", data)
	}
	ctx, cancel := context.WithTimeout(context.Background(), failsafe)
	defer cancel()
	stopped = true
	if err := r.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v: the handler's context did not end with the drain", err)
	}

	var c calls
	work(t, h.Queue, w, c.handler(w, nil))
	eventually(t, c.has("x"), "the drained job's redelivery")
	if got := c.of("x")[0].attempt; got < 2 {
		t.Errorf("redelivered as attempt %d, want a redelivery", got)
	}
}
