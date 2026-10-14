package messaging_test

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	libconfig "github.com/standards-lab/go-core/config"
	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-core/reactor"
	"github.com/standards-lab/go-messaging"
	"github.com/standards-lab/go-messaging/memory"
	"github.com/standards-lab/sqlate"
	"github.com/standards-lab/sqlate/sqltest"
)

// settleTime is how long a test lets a gated reactor run before it checks
// that the reactor did nothing.
const settleTime = 50 * time.Millisecond

// hold holds g exclusively, as an operation that pauses the background work
// does, and returns the release, which the test's end also calls.
func hold(t *testing.T, g *reactor.Gate) func() {
	t.Helper()
	release, err := g.Exclusive(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	return release
}

// failingGate is a gate whose Shared fails with err.
type failingGate struct{ err error }

func (g failingGate) Shared(context.Context) (func(), error) { return nil, g.err }

// watchedGate is a reactor.Gate that notes, as each shared hold begins and
// ends, the database operation the relay ran last, so a test sees what the
// relay ran outside the gate.
type watchedGate struct {
	reactor.Gate

	mu   sync.Mutex
	rec  *sqltest.Recorder
	seen []string
}

func (g *watchedGate) note(at string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	last := "none"
	if ops := g.rec.Ops(); len(ops) > 0 {
		last = string(ops[len(ops)-1])
	}
	g.seen = append(g.seen, at+" after "+last)
}

func (g *watchedGate) Shared(ctx context.Context) (func(), error) {
	release, err := g.Gate.Shared(ctx)
	if err != nil {
		return nil, err
	}
	g.note("held")
	return func() { g.note("released"); release() }, nil
}

func (g *watchedGate) holds() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.seen)
}

// gatedRelay starts the relay of a runtime built with opts over a scripted
// database, polling hourly so each test runs one pass and the last; it
// returns the relay, the script's recorder, the broker, and the failures.
func gatedRelay(t *testing.T, opts []messaging.Option, script ...sqltest.Response) (*reactor.Reactor[event.Event], *sqltest.Recorder, *fakeBroker, *failures) {
	t.Helper()
	ob, in := engines(t)
	pool, rec := sqltest.Open(t, script...)
	cfg := config(t)
	cfg.RelayPoll = libconfig.Duration(time.Hour)
	b := &fakeBroker{}
	f := &failures{}
	rt, err := messaging.New(cfg, b, ob, in, time.Second, slog.New(slog.DiscardHandler), append(opts, messaging.OnFailure(f.hook))...)
	if err != nil {
		t.Fatal(err)
	}
	r := rt.Relay(sqlate.Wrap(pool, sqltest.Dialect{}))
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	// A relay that waits on a gate the test still holds cannot drain, so
	// the cleanup's shutdown gives up after the failsafe.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), failsafe)
		defer cancel()
		_ = r.Shutdown(ctx)
	})
	return r, rec, b, f
}

// published counts the events b was handed.
func (b *fakeBroker) published() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.deadlines)
}

// A gated relay opens no transaction while an exclusive holder holds the
// gate, and publishes once it is released. It holds the gate for each row
// from before the row's transaction begins until after it commits or rolls
// back, so it runs nothing outside the gate.
func TestGatedRelayWaitsForTheExclusiveHolder(t *testing.T) {
	g := &watchedGate{}
	release := hold(t, &g.Gate)
	r, rec, b, f := gatedRelay(t, []messaging.Option{messaging.Gated(g)},
		claimEvent(t, 1, event1), sqltest.Response{Affected: 1}, sqltest.Response{Columns: claimColumns},
		// The last pass, on shutdown.
		sqltest.Response{Columns: claimColumns},
	)
	// The relay waits on the gate, so it cannot yet look at rec.
	g.mu.Lock()
	g.rec = rec
	g.mu.Unlock()
	time.Sleep(settleTime)
	if ops := rec.Ops(); len(ops) != 0 {
		t.Fatalf("while the gate was held exclusively, the relay ran %v, want nothing", ops)
	}
	if !r.Ready() {
		t.Error("a relay waiting on the gate is not ready")
	}
	release()
	waitFor(t, "the publish", func() bool { return b.published() == 1 })
	// The pass ends on the empty claim; once the relay is asleep, an
	// exclusive holder is admitted at once and sees both rows' transactions
	// settled.
	waitFor(t, "the pass's end", func() bool { return rec.Pending() == 1 })
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	again, err := g.Exclusive(ctx)
	if err != nil {
		t.Fatalf("the relay did not let the gate go after its pass: %v", err)
	}
	again()
	ops := rec.Ops()
	if want := []sqltest.Op{sqltest.OpBegin, sqltest.OpQuery, sqltest.OpExec, sqltest.OpCommit, sqltest.OpBegin, sqltest.OpQuery, sqltest.OpRollback}; !slices.Equal(ops, want) {
		t.Errorf("relay ran %v, want %v", ops, want)
	}
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.all(); len(got) != 0 {
		t.Errorf("hook got %+v, want nothing", got)
	}
	// Two rows in the first pass, and the last pass's one.
	want := []string{
		"held after none", "released after commit",
		"held after commit", "released after rollback",
		"held after rollback", "released after rollback",
	}
	if got := g.holds(); !slices.Equal(got, want) {
		t.Errorf("gate holds = %v, want %v", got, want)
	}
}

// A relay still waiting on the gate when it is stopped claims no row and
// reports nothing: the stop ends its wait, and the last pass's own bound
// ends that pass's, inside the drain, so the shutdown does not cancel it.
func TestGatedRelayStopEndsTheWaitAndReportsNothing(t *testing.T) {
	var g reactor.Gate
	hold(t, &g)
	r, rec, b, f := gatedRelay(t, []messaging.Option{messaging.Gated(&g)})
	time.Sleep(settleTime)
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	if err := r.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if ops := rec.Ops(); len(ops) != 0 {
		t.Errorf("relay ran %v, want nothing", ops)
	}
	if n := b.published(); n != 0 {
		t.Errorf("relay published %d events, want none", n)
	}
	if got := f.all(); len(got) != 0 {
		t.Errorf("hook got %+v, want nothing", got)
	}
}

// A gate that fails other than by its wait ending fails the pass, which is
// reported as a relay failure, before any transaction; the relay stays
// ready, since the database did not fail.
func TestGatedRelayGateFailureIsReported(t *testing.T) {
	broken := errors.New("gate broken")
	r, rec, _, f := gatedRelay(t, []messaging.Option{messaging.Gated(failingGate{broken})})
	waitFor(t, "the failure", func() bool { return len(f.all()) > 0 })
	if got := f.all()[0]; got.Kind != messaging.FailureRelay || !errors.Is(got.Err, broken) {
		t.Errorf("failure = %+v, want the gate's failure as a relay failure", got)
	}
	if !r.Ready() {
		t.Error("a gate failure made the relay not ready")
	}
	if ops := rec.Ops(); len(ops) != 0 {
		t.Errorf("relay ran %v, want nothing", ops)
	}
}

// gatedConsumer builds a runtime over b with opts whose failures go to f
// and log to log, and returns an unstarted consumer of sub that sends each
// delivery's attempt on the channel returned.
func gatedConsumer(t *testing.T, b messaging.Broker, f *failures, log *syncBuffer, sub messaging.Subscription, opts ...messaging.Option) (*reactor.Reactor[event.Event], <-chan int) {
	t.Helper()
	ob, in := engines(t)
	rt, err := messaging.New(config(t), b, ob, in, time.Second, slog.New(slog.NewTextHandler(log, nil)), append(opts, messaging.OnFailure(f.hook))...)
	if err != nil {
		t.Fatal(err)
	}
	attempts := make(chan int, 16)
	r, err := rt.Consume(sub, func(ctx context.Context, _ payload, _ messaging.Claim) error {
		attempts <- messaging.Attempt(ctx)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return r, attempts
}

// A gated consumer's handler is not called while an exclusive holder holds
// the gate, and is called on the same delivery once the gate is released.
func TestGatedConsumerWaitsForTheExclusiveHolder(t *testing.T) {
	var g reactor.Gate
	release := hold(t, &g)
	b := memory.New()
	var f failures
	var log syncBuffer
	r, attempts := gatedConsumer(t, b, &f, &log, messaging.Subscription{Name: "consumer"}, messaging.Gated(&g))
	start(t, r)
	if err := b.Publish(t.Context(), event1); err != nil {
		t.Fatal(err)
	}
	select {
	case a := <-attempts:
		t.Fatalf("handler called on attempt %d while the gate was held exclusively", a)
	case <-time.After(settleTime):
	}
	release()
	awaitAttempts(t, attempts, 1)
	if got := f.all(); len(got) != 0 {
		t.Errorf("hook got %+v, want nothing", got)
	}
}

// A drain that ends a delivery's wait on the gate hands the handler
// nothing, logs and reports nothing, and leaves the event unacknowledged:
// the consumer's next member receives it again, on its second attempt. The
// drain ends the wait itself, so the shutdown cancels no handler.
func TestGatedConsumerDrainLeavesTheEventForRedelivery(t *testing.T) {
	var g reactor.Gate
	release := hold(t, &g)
	b := memory.New()
	var f failures
	var log syncBuffer
	sub := messaging.Subscription{Name: "consumer", RetryDelay: time.Millisecond}
	first, attempts := gatedConsumer(t, b, &f, &log, sub, messaging.Gated(&g))
	if err := first.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(t.Context(), event1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(settleTime)
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	if err := first.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case a := <-attempts:
		t.Fatalf("handler called on attempt %d although the drain ended its wait", a)
	default:
	}
	if got := f.all(); len(got) != 0 {
		t.Errorf("hook got %+v, want nothing", got)
	}
	if strings.Contains(log.String(), "event consumed") {
		t.Errorf("the unhandled delivery was logged:\n%s", log.String())
	}

	release()
	next, again := gatedConsumer(t, b, &f, &log, sub, messaging.Gated(&g))
	start(t, next)
	awaitAttempts(t, again, 2)
}

// Without Gated, or with a nil gate, neither the relay nor a consumer
// holds any gate, so a held one stops neither.
func TestUngatedRuntimeHoldsNoGate(t *testing.T) {
	var g reactor.Gate
	hold(t, &g)
	for name, opts := range map[string][]messaging.Option{
		"without Gated":   nil,
		"with a nil gate": {messaging.Gated(nil)},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, rb, f := gatedRelay(t, opts,
				claimEvent(t, 1, event1), sqltest.Response{Affected: 1}, sqltest.Response{Columns: claimColumns},
				sqltest.Response{Columns: claimColumns},
			)
			waitFor(t, "the publish", func() bool { return rb.published() == 1 })

			b := memory.New()
			var log syncBuffer
			r, attempts := gatedConsumer(t, b, f, &log, messaging.Subscription{Name: "consumer"}, opts...)
			start(t, r)
			if err := b.Publish(t.Context(), event1); err != nil {
				t.Fatal(err)
			}
			awaitAttempts(t, attempts, 1)
			if got := f.all(); len(got) != 0 {
				t.Errorf("hook got %+v, want nothing", got)
			}
		})
	}
}
