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

// gatedRelay starts the relay of a runtime built with gate over a scripted
// database, polling hourly so each test runs one pass and the last; it
// returns the relay, the script's recorder, the broker, and the failures.
func gatedRelay(t *testing.T, gate messaging.Gate, script ...sqltest.Response) (*reactor.Reactor[event.Event], *sqltest.Recorder, *fakeBroker, *failures) {
	t.Helper()
	pool, rec := sqltest.Open(t, script...)
	b := &fakeBroker{}
	f := &failures{}
	s := setup(t, b, slog.New(slog.DiscardHandler))
	s.Config.RelayPoll = libconfig.Duration(time.Hour)
	s.OnFailure, s.Gate = f.hook, gate
	r := build(t, s).Relay(sqlate.Wrap(pool, sqltest.Dialect{}))
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
func TestRelayGateWaitsForTheExclusiveHolder(t *testing.T) {
	g := &watchedGate{}
	release := hold(t, &g.Gate)
	r, rec, b, f := gatedRelay(t, g,
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
func TestRelayGateStopEndsTheWaitAndReportsNothing(t *testing.T) {
	var g reactor.Gate
	hold(t, &g)
	r, rec, b, f := gatedRelay(t, &g)
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
func TestRelayGateFailureIsReported(t *testing.T) {
	broken := errors.New("gate broken")
	r, rec, _, f := gatedRelay(t, failingGate{broken})
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

// gatedConsumer builds a runtime over b with gate whose failures go to f
// and log to log, and returns an unstarted consumer of sub that sends each
// delivery's attempt on the channel returned.
func gatedConsumer(t *testing.T, b messaging.Broker, f *failures, log *syncBuffer, sub messaging.Subscription, gate messaging.Gate) (*reactor.Reactor[event.Event], <-chan int) {
	t.Helper()
	s := setup(t, b, slog.New(slog.NewTextHandler(log, nil)))
	s.OnFailure, s.Gate = f.hook, gate
	attempts := make(chan int, 16)
	r, err := build(t, s).Consume(sub, func(ctx context.Context, _ payload, _ messaging.Claim) error {
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
func TestConsumerGateWaitsForTheExclusiveHolder(t *testing.T) {
	var g reactor.Gate
	release := hold(t, &g)
	b := memory.New()
	var f failures
	var log syncBuffer
	r, attempts := gatedConsumer(t, b, &f, &log, messaging.Subscription{Name: "consumer"}, &g)
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
func TestConsumerGateDrainLeavesTheEventForRedelivery(t *testing.T) {
	var g reactor.Gate
	release := hold(t, &g)
	b := memory.New()
	var f failures
	var log syncBuffer
	sub := messaging.Subscription{Name: "consumer", RetryDelay: time.Millisecond}
	first, attempts := gatedConsumer(t, b, &f, &log, sub, &g)
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
	next, again := gatedConsumer(t, b, &f, &log, sub, &g)
	start(t, next)
	awaitAttempts(t, again, 2)
}

// A gate that fails other than by its wait ending is the delivery's
// outcome, as a transient error of the handler's would be: the handler is
// never called, each delivery is logged as retried, and the last is
// reported as exhausted with the gate's error.
func TestConsumerGateFailureExhausts(t *testing.T) {
	broken := errors.New("gate broken")
	b := memory.New()
	var f failures
	var log syncBuffer
	sub := messaging.Subscription{Name: "consumer", MaxDeliver: 2, RetryDelay: time.Millisecond}
	r, attempts := gatedConsumer(t, b, &f, &log, sub, failingGate{broken})
	start(t, r)
	if err := b.Publish(t.Context(), event1); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the exhaustion", func() bool { return len(f.all()) > 0 })
	select {
	case a := <-attempts:
		t.Fatalf("handler called on attempt %d although the gate failed", a)
	default:
	}
	got := f.all()
	if len(got) != 1 || got[0].Kind != messaging.FailureExhausted || got[0].Attempt != 2 || !errors.Is(got[0].Err, broken) {
		t.Errorf("hook got %+v, want one exhaustion on attempt 2 with the gate's error", got)
	}
	if n := strings.Count(log.String(), "outcome=retried"); n != 2 {
		t.Errorf("logged %d retried deliveries, want 2:\n%s", n, log.String())
	}
}

// Without a gate, neither the relay nor a consumer holds any, so a held
// one stops neither.
func TestRuntimeWithoutAGateHoldsNone(t *testing.T) {
	var g reactor.Gate
	hold(t, &g)
	_, _, rb, f := gatedRelay(t, nil,
		claimEvent(t, 1, event1), sqltest.Response{Affected: 1}, sqltest.Response{Columns: claimColumns},
		sqltest.Response{Columns: claimColumns},
	)
	waitFor(t, "the publish", func() bool { return rb.published() == 1 })

	b := memory.New()
	var log syncBuffer
	r, attempts := gatedConsumer(t, b, f, &log, messaging.Subscription{Name: "consumer"}, nil)
	start(t, r)
	if err := b.Publish(t.Context(), event1); err != nil {
		t.Fatal(err)
	}
	awaitAttempts(t, attempts, 1)
	if got := f.all(); len(got) != 0 {
		t.Errorf("hook got %+v, want nothing", got)
	}
}

// nilGate is a gate whose nil pointer is usable: Shared fails with
// errNilGate, so a test sees that a typed nil was held.
type nilGate struct{}

var errNilGate = errors.New("nil gate held")

func (*nilGate) Shared(context.Context) (func(), error) { return nil, errNilGate }

// A Gate that is a non-nil interface holding a nil pointer is set: the
// relay and a consumer both hold it, and its failure reaches the hook.
func TestTypedNilGateIsHeld(t *testing.T) {
	var gate *nilGate
	_, rec, _, f := gatedRelay(t, gate)
	waitFor(t, "the relay's failure", func() bool { return len(f.all()) > 0 })
	if got := f.all()[0]; got.Kind != messaging.FailureRelay || !errors.Is(got.Err, errNilGate) {
		t.Errorf("relay failure = %+v, want the typed-nil gate's failure", got)
	}
	if ops := rec.Ops(); len(ops) != 0 {
		t.Errorf("relay ran %v, want nothing", ops)
	}

	b := memory.New()
	var cf failures
	var log syncBuffer
	sub := messaging.Subscription{Name: "consumer", MaxDeliver: 1}
	r, attempts := gatedConsumer(t, b, &cf, &log, sub, gate)
	start(t, r)
	if err := b.Publish(t.Context(), event1); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the consumer's exhaustion", func() bool { return len(cf.all()) > 0 })
	select {
	case a := <-attempts:
		t.Fatalf("handler called on attempt %d although the gate failed", a)
	default:
	}
	if got := cf.all(); len(got) != 1 || got[0].Kind != messaging.FailureExhausted || !errors.Is(got[0].Err, errNilGate) {
		t.Errorf("consumer failures = %+v, want one exhaustion with the typed-nil gate's failure", got)
	}
}
