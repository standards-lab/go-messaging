//go:build integration

package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-core/reactor"
	"github.com/standards-lab/go-messaging"
	"github.com/standards-lab/go-messaging/memory"
	"github.com/standards-lab/go-messaging/postgres"
	"github.com/standards-lab/sqlate"
)

// claim claims e for consumer in a transaction of its own, rolling it back
// when rollback is set.
func claim(t *testing.T, db *sqlate.DB, consumer string, e event.Event, rollback bool) bool {
	t.Helper()
	// Transact returns the zero value with an error, so the claim's result
	// leaves the transaction through first.
	undo := errors.New("roll back")
	var first bool
	_, err := sqlate.Transact(t.Context(), db, func(tx *sqlate.Tx) (struct{}, error) {
		var err error
		if first, err = in.Claim(t.Context(), tx, consumer, e); err != nil {
			return struct{}{}, err
		}
		if rollback {
			return struct{}{}, undo
		}
		return struct{}{}, nil
	})
	if err != nil && !errors.Is(err, undo) {
		t.Fatalf("claim: %v", err)
	}
	return first
}

func TestClaimIsFirstOnce(t *testing.T) {
	db := migrated(t)
	if !claim(t, db, "billing", tick("1"), false) {
		t.Fatal("the first claim was not first")
	}
	if claim(t, db, "billing", tick("1"), false) {
		t.Fatal("a second claim of a handled event was first")
	}
}

func TestRolledBackClaimIsReleased(t *testing.T) {
	db := migrated(t)
	if !claim(t, db, "billing", tick("1"), true) {
		t.Fatal("the first claim was not first")
	}
	if !claim(t, db, "billing", tick("1"), false) {
		t.Fatal("a claim after a rollback was not first")
	}
}

func TestConsumersClaimIndependently(t *testing.T) {
	db := migrated(t)
	for _, consumer := range []string{"billing", "audit"} {
		if !claim(t, db, consumer, tick("1"), false) {
			t.Fatalf("%s's first claim was not first", consumer)
		}
	}
}

func TestClaimKeysOnSourceAndID(t *testing.T) {
	db := migrated(t)
	other := tick("1")
	other.Source = "/elsewhere"
	claim(t, db, "billing", tick("1"), false)
	if !claim(t, db, "billing", other, false) {
		t.Fatal("an event with the same id from another source was not first")
	}
}

func TestClaimRequiresAConsumer(t *testing.T) {
	db := migrated(t)
	tx, err := db.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := in.Claim(t.Context(), tx, "", tick("1")); err == nil {
		t.Fatal("a claim without a consumer succeeded")
	}
}

// A handler that outlives AckWait holds its claim only until its context's
// deadline, which is the delivery's AckWait: the deadline rolls its
// transaction back, so the redelivery, to another member of the group,
// claims the event and does the work once. The redelivery's claim waits on
// the first handler's row lock rather than failing, and under READ
// COMMITTED proceeds once the lock is released.
func TestClaimAcrossAckWait(t *testing.T) {
	db := migrated(t)
	b := memory.New()
	sub := messaging.Subscription{Name: "billing", AckWait: 200 * time.Millisecond}
	var mu sync.Mutex
	var attempts, work int
	firsts := make([]bool, 0, 2)
	handler := func(ctx context.Context, e event.Event) error {
		mu.Lock()
		attempts++
		n := attempts
		mu.Unlock()
		_, err := sqlate.Transact(ctx, db, func(tx *sqlate.Tx) (struct{}, error) {
			first, err := in.Claim(ctx, tx, sub.Name, e)
			if err != nil {
				return struct{}{}, err
			}
			mu.Lock()
			firsts = append(firsts, first)
			mu.Unlock()
			if !first {
				return struct{}{}, nil
			}
			if n == 1 {
				<-ctx.Done() // overrun AckWait, holding the claim
				return struct{}{}, ctx.Err()
			}
			mu.Lock()
			work++
			mu.Unlock()
			return struct{}{}, nil
		})
		return err
	}
	for range 2 {
		src, err := b.Subscribe(sub, nil)
		if err != nil {
			t.Fatal(err)
		}
		r := reactor.New(src, handler)
		if err := r.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { stop(r) })
		eventually(t, "the member to be receiving", r.Ready)
	}
	if err := b.Publish(t.Context(), tick("1")); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the redelivery to do the work", func() bool { mu.Lock(); defer mu.Unlock(); return work == 1 })
	time.Sleep(2 * sub.AckWait)
	mu.Lock()
	defer mu.Unlock()
	if attempts != 2 || work != 1 {
		t.Errorf("%d deliveries did the work %d times, want 2 deliveries and the work once", attempts, work)
	}
	if !slices.Equal(firsts, []bool{true, true}) {
		t.Errorf("claims were first %v, want both first: the overrun's rollback releases its claim", firsts)
	}
	if claim(t, db, sub.Name, tick("1"), false) {
		t.Error("the event was claimable after the redelivery committed")
	}
}

// A consumer the runtime builds claims each event under its subscription's
// name, in the consumer's own transaction: the claim it hands over is
// first once for the delivered event, and the inbox holds that consumer
// and event.
func TestConsumeClaimsUnderTheSubscription(t *testing.T) {
	db := migrated(t)
	b := memory.New()
	cfg := messaging.Config{Source: "/test"}
	if err := cfg.Finalize(""); err != nil {
		t.Fatal(err)
	}
	rt, err := messaging.New(cfg, b, postgres.Outbox(), postgres.Inbox(), time.Second, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	firsts := make(chan []bool, 1)
	sub := messaging.Subscription{Name: "billing", Types: []string{"lab.demo.ticked"}}
	r, err := rt.Consume(sub, func(ctx context.Context, _ struct{}, claim messaging.Claim) error {
		var got []bool
		for range 2 {
			first, err := sqlate.Transact(ctx, db, func(tx *sqlate.Tx) (bool, error) { return claim(ctx, tx) })
			if err != nil {
				return err
			}
			got = append(got, first)
		}
		firsts <- got
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Shutdown(context.Background()) })

	e := event.Event{ID: "7", Source: "/test", Type: "lab.demo.ticked", DataContentType: "application/json", Data: []byte("{}")}
	if err := b.Publish(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-firsts:
		if !slices.Equal(got, []bool{true, false}) {
			t.Errorf("claims = %v, want first once", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the consumer")
	}
	if claim(t, db, "billing", e, false) {
		t.Error("the inbox does not hold the event under the subscription's name")
	}
	if !claim(t, db, "another", e, false) {
		t.Error("the claim was not bound to its own consumer")
	}
}

// The runtime logs the traffic it carries: the relay each event it
// publishes, and a consumer each delivery with its outcome, so a delivery
// of an event the inbox already holds is logged as a repeat.
func TestRuntimeLogsTheTraffic(t *testing.T) {
	db := migrated(t)
	b := memory.New()
	var log lockedBuffer
	cfg := messaging.Config{Source: "/test"}
	if err := cfg.Finalize(""); err != nil {
		t.Fatal(err)
	}
	rt, err := messaging.New(cfg, b, postgres.Outbox(), postgres.Inbox(), time.Second, slog.New(slog.NewTextHandler(&log, nil)))
	if err != nil {
		t.Fatal(err)
	}
	handled := make(chan event.Event, 2)
	sub := messaging.Subscription{Name: "billing", Types: []string{"lab.demo.tick"}}
	consumer, err := rt.Consume(sub, func(ctx context.Context, _ struct{}, claim messaging.Claim) error {
		_, err := sqlate.Transact(ctx, db, func(tx *sqlate.Tx) (bool, error) { return claim(ctx, tx) })
		handled <- event.Event{}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []*reactor.Reactor[event.Event]{consumer, rt.Relay(db)} {
		if err := r.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { stop(r) })
	}

	// The inbox already holds 2, as it would once a delivery of 2 had
	// committed, so its delivery is a repeat.
	claim(t, db, sub.Name, tick("2"), false)
	emit(t, db, tick("1"), tick("2"))
	await(t, handled)
	await(t, handled)
	eventually(t, "the repeat's log line", func() bool { return strings.Contains(log.String(), "outcome=repeat") })
	for _, want := range []string{
		`msg="event published" type=lab.demo.tick event=1`,
		`msg="event published" type=lab.demo.tick event=2`,
		`msg="event consumed" consumer=billing type=lab.demo.tick event=1 subject="" outcome=handled`,
		`msg="event consumed" consumer=billing type=lab.demo.tick event=2 subject="" outcome=repeat`,
	} {
		if !strings.Contains(log.String(), want) {
			t.Errorf("the log lacks %s:\n%s", want, log.String())
		}
	}
}

// await waits for a delivery on ch, within the failsafe.
func await(t *testing.T, ch <-chan event.Event) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(failsafe):
		t.Fatal("timed out waiting for a delivery")
	}
}

// lockedBuffer serializes the reactors' writes and the test's read.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
