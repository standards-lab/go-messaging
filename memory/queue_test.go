package memory_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-messaging"
	"github.com/standards-lab/go-messaging/memory"
	"github.com/standards-lab/go-messaging/messagingtest"
)

func TestQueueConformance(t *testing.T) {
	messagingtest.RunQueue(t, func(*testing.T) messagingtest.QueueHarness {
		q, c := memory.NewQueueOnClock()
		return messagingtest.QueueHarness{Queue: q, Unit: time.Second, Advance: c.Advance}
	})
}

const failsafe = 5 * time.Second

type worked struct {
	mu   sync.Mutex
	ids  []string
	dead []messaging.DeadLetter
}

func (w *worked) add(ctx context.Context, e event.Event) {
	d, _ := messaging.DeadLetterOf(ctx)
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ids = append(w.ids, e.ID)
	w.dead = append(w.dead, d)
}

func (w *worked) count(id string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, v := range w.ids {
		if v == id {
			n++
		}
	}
	return n
}

func (w *worked) deadOf(id string) []messaging.DeadLetter {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []messaging.DeadLetter
	for i, v := range w.ids {
		if v == id {
			out = append(out, w.dead[i])
		}
	}
	return out
}

func receive(t *testing.T, q *memory.Queue, w messaging.Worker, fn func(context.Context, event.Event) error) {
	t.Helper()
	src, err := q.Subscribe(w, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = src.Receive(ctx, fn)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(failsafe):
			t.Error("Receive did not return after its context ended")
		}
	})
	waitFor(t, src.Ready, "the worker to be receiving")
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(failsafe)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

var (
	jobs = messaging.Worker{Name: "jobs", MaxDeliver: 1, AckWait: 2 * time.Second, DeadLetter: "jobs.dead"}
	dead = messaging.Worker{Name: "dead", Subject: "jobs.dead", MaxDeliver: 1, DeadLetter: "jobs.dead.dead"}
)

func TestQueueLostExtensionEndsHandler(t *testing.T) {
	q, c := memory.NewQueueOnClock()
	started := make(chan context.Context, 1)
	var ran, dl worked
	receive(t, q, jobs, func(ctx context.Context, e event.Event) error {
		ran.add(ctx, e)
		if e.ID == "x" {
			started <- ctx
			<-ctx.Done()
		}
		return nil
	})
	receive(t, q, dead, func(ctx context.Context, e event.Event) error {
		dl.add(ctx, e)
		return nil
	})
	if err := q.Enqueue(context.Background(), event.Event{ID: "x", Source: "/t", Type: "t"}); err != nil {
		t.Fatal(err)
	}
	var hctx context.Context
	select {
	case hctx = <-started:
	case <-time.After(failsafe):
		t.Fatal("timed out waiting for the handler")
	}
	q.LoseExtension()
	c.Advance(jobs.AckWait)
	select {
	case <-hctx.Done():
	case <-time.After(failsafe):
		t.Fatal("the handler's context did not end AckWait after its extension was lost")
	}
	waitFor(t, func() bool { return dl.count("x") > 0 }, "the dead letter")
	if got, want := dl.deadOf("x")[0], (messaging.DeadLetter{Attempts: 1}); got != want {
		t.Errorf("DeadLetterOf = %+v, want %+v: the final attempt's outcome was lost", got, want)
	}
	if n := ran.count("x"); n != 1 {
		t.Errorf("the handler ran %d times, want once: the redelivery past the final attempt dead-letters", n)
	}
}

func TestQueueDeadLetterStoredOnce(t *testing.T) {
	q, _ := memory.NewQueueOnClock()
	var dl worked
	receive(t, q, jobs, func(context.Context, event.Event) error { return errors.New("always") })
	receive(t, q, dead, func(ctx context.Context, e event.Event) error {
		dl.add(ctx, e)
		return nil
	})
	q.KeepNextDeadLettered()
	for _, id := range []string{"x", "y"} {
		if err := q.Enqueue(context.Background(), event.Event{ID: id, Source: "/t", Type: "t"}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return dl.count("y") > 0 }, "the next dead letter")
	got := dl.deadOf("x")
	if len(got) != 1 {
		t.Fatalf("the dead letter was stored %d times, want once", len(got))
	}
	if want := (messaging.DeadLetter{Attempts: 1, Errored: true, Err: "always"}); got[0] != want {
		t.Errorf("DeadLetterOf = %+v, want the first dead-lettering's %+v", got[0], want)
	}
}

func TestQueueOnWallClock(t *testing.T) {
	q := memory.NewQueue()
	got := make(chan string, 1)
	receive(t, q, jobs, func(_ context.Context, e event.Event) error {
		got <- e.ID
		return nil
	})
	if err := q.Enqueue(context.Background(), event.Event{ID: "x", Source: "/t", Type: "t"}); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-got:
		if id != "x" {
			t.Errorf("handled %q, want x", id)
		}
	case <-time.After(failsafe):
		t.Fatal("timed out waiting for the job")
	}
}
