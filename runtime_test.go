package messaging_test

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	libconfig "github.com/standards-lab/go-core/config"
	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-core/reactor"
	"github.com/standards-lab/go-messaging"
	"github.com/standards-lab/go-messaging/inbox"
	"github.com/standards-lab/go-messaging/memory"
	"github.com/standards-lab/go-messaging/outbox"
	"github.com/standards-lab/sqlate"
	"github.com/standards-lab/sqlate/query"
	"github.com/standards-lab/sqlate/sqltest"
)

const failsafe = 2 * time.Second

// engines compiles the outbox's and the inbox's statements over sqlate's
// test dialect, so the runtime is built without a driver.
func engines(t *testing.T) (outbox.Engine, inbox.Engine) {
	t.Helper()
	fsys := fstest.MapFS{}
	for name, text := range map[string]string{
		"emit":           "INSERT INTO o (s, id, h, d) VALUES ({{source}}, {{id}}, {{header}}, {{data}})",
		"claim_row":      "SELECT seq, header, data FROM o",
		"mark_published": "UPDATE o SET p = 1 WHERE seq = {{seq}}",
		"quarantine":     "UPDATE o SET q = now(), r = {{reason}} WHERE seq = {{seq}}",
		"claim":          "INSERT INTO i (c, s, id) VALUES ({{consumer}}, {{source}}, {{id}})",
	} {
		fsys["s/"+name+".sql"] = &fstest.MapFile{Data: []byte("--| tier: standard\n" + text)}
	}
	s := query.MustCatalog(query.Patterns()).MustCompile(fsys, "s", sqltest.Dialect{})
	return outbox.Engine{
		Emit:          s.Statement("emit"),
		ClaimRow:      s.Statement("claim_row"),
		MarkPublished: s.Statement("mark_published"),
		Quarantine:    s.Statement("quarantine"),
	}, inbox.Engine{Claim: s.Statement("claim")}
}

// config is a finalized configuration with the source /test.
func config(t *testing.T) messaging.Config {
	t.Helper()
	cfg := messaging.Config{Source: "/test"}
	if err := cfg.Finalize(""); err != nil {
		t.Fatal(err)
	}
	return cfg
}

// New refuses an engine that is missing a statement, a configuration that
// is not finalized, and a drain timeout that is not positive.
func TestNewChecksItsInputs(t *testing.T) {
	ob, in := engines(t)
	cfg := config(t)
	noTimeout := cfg
	noTimeout.RelayTimeout = 0
	for name, build := range map[string]func() (*messaging.Runtime, error){
		"an empty outbox engine": func() (*messaging.Runtime, error) {
			return messaging.New(cfg, memory.New(), outbox.Engine{}, in, time.Second, slog.Default())
		},
		"an empty inbox engine": func() (*messaging.Runtime, error) {
			return messaging.New(cfg, memory.New(), ob, inbox.Engine{}, time.Second, slog.Default())
		},
		"a configuration without its poll": func() (*messaging.Runtime, error) {
			return messaging.New(messaging.Config{Source: "/test", RelayTimeout: cfg.RelayTimeout}, memory.New(), ob, in, time.Second, slog.Default())
		},
		"a configuration without its relay timeout": func() (*messaging.Runtime, error) {
			return messaging.New(noTimeout, memory.New(), ob, in, time.Second, slog.Default())
		},
		"a zero drain timeout": func() (*messaging.Runtime, error) {
			return messaging.New(cfg, memory.New(), ob, in, 0, slog.Default())
		},
	} {
		if _, err := build(); err == nil {
			t.Errorf("New accepted %s", name)
		}
	}
	rt, err := messaging.New(cfg, memory.New(), ob, in, time.Second, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if rt.Recorder == nil {
		t.Error("New left the recorder unset")
	}
}

type payload struct {
	N int `json:"n"`
}

// Consume decodes each event's data into the consumer's type and hands it
// over with a claim; an event whose data does not decode never reaches the
// consumer and is not redelivered, and neither is one the consumer refuses
// permanently, so the next event still arrives. Each delivery is logged
// once with its outcome: a refusal at warn with its error, a failure the
// broker redelivers as retried, and a success as handled.
func TestConsume(t *testing.T) {
	ob, in := engines(t)
	b := memory.New()
	var log syncBuffer
	rt, err := messaging.New(config(t), b, ob, in, time.Second, slog.New(slog.NewTextHandler(&log, nil)))
	if err != nil {
		t.Fatal(err)
	}
	type got struct {
		data  payload
		claim bool
	}
	calls := make(chan got, 4)
	var failed sync.Once
	sub := messaging.Subscription{Name: "consumer", Types: []string{"t"}, RetryDelay: 10 * time.Millisecond}
	r, err := rt.Consume(sub, func(_ context.Context, d payload, claim messaging.Claim) error {
		if d.N == 9 {
			return event.Permanent(errors.New("nine is refused"))
		}
		retry := false
		if d.N == 5 {
			failed.Do(func() { retry = true })
		}
		if retry {
			return errors.New("five fails once")
		}
		calls <- got{data: d, claim: claim != nil}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	start(t, r)

	for _, e := range []event.Event{
		{ID: "bad", Source: "/test", Type: "t", DataContentType: "application/json", Data: []byte("{")},
		{ID: "refused", Source: "/test", Type: "t", DataContentType: "application/json", Data: []byte(`{"n":9}`)},
		{ID: "flaky", Source: "/test", Type: "t", DataContentType: "application/json", Data: []byte(`{"n":5}`)},
		{ID: "good", Source: "/test", Type: "t", Subject: "s1", DataContentType: "application/json", Data: []byte(`{"n":7}`)},
	} {
		if err := b.Publish(t.Context(), e); err != nil {
			t.Fatal(err)
		}
	}
	handled := map[int]bool{}
	for len(handled) < 2 {
		select {
		case c := <-calls:
			if handled[c.data.N] || c.data.N != 5 && c.data.N != 7 || !c.claim {
				t.Fatalf("consumer got %+v, want n=5 and n=7 once each, with a claim", c)
			}
			handled[c.data.N] = true
		case <-time.After(failsafe):
			t.Fatal("timed out waiting for the good events")
		}
	}
	select {
	case c := <-calls:
		t.Errorf("consumer called again with %+v", c)
	case <-time.After(50 * time.Millisecond):
	}

	lines := strings.Split(strings.TrimSpace(log.String()), "\n")
	outcomes := func(id string) []string {
		var out []string
		for _, l := range lines {
			if strings.Contains(l, "event="+id+" ") {
				_, o, _ := strings.Cut(l, "outcome=")
				o, _, _ = strings.Cut(o, " ")
				out = append(out, o)
			}
		}
		return out
	}
	for id, want := range map[string][]string{
		"bad": {"refused"}, "refused": {"refused"}, "flaky": {"retried", "handled"}, "good": {"handled"},
	} {
		if got := outcomes(id); !slices.Equal(got, want) {
			t.Errorf("%s's deliveries were logged as %v, want %v:\n%s", id, got, want, log.String())
		}
	}
	for _, want := range []string{
		`level=WARN msg="event refused" consumer=consumer type=t event=refused`,
		`error="permanent: nine is refused"`,
		`msg="event consumed" consumer=consumer type=t event=flaky subject="" outcome=retried error="five fails once"`,
		`level=INFO msg="event consumed" consumer=consumer type=t event=good subject=s1 outcome=handled`,
	} {
		if !strings.Contains(log.String(), want) {
			t.Errorf("the log lacks %s:\n%s", want, log.String())
		}
	}
	if strings.Contains(log.String(), "messaging failure") {
		t.Errorf("an unlimited subscription reported a failure:\n%s", log.String())
	}

	if _, err := rt.Consume(messaging.Subscription{Name: "a.b"}, func(context.Context, payload, messaging.Claim) error { return nil }); err == nil {
		t.Error("Consume accepted an invalid subscription")
	}
}

// failures collects what a runtime's hook is given.
type failures struct {
	mu  sync.Mutex
	got []messaging.Failure
}

func (f *failures) hook(_ context.Context, fl messaging.Failure) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, fl)
}

func (f *failures) all() []messaging.Failure {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.got)
}

// waitFor fails the test unless cond holds within the failsafe.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(failsafe)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// start starts r and shuts it down when the test ends.
func start(t *testing.T, r *reactor.Reactor[event.Event]) {
	t.Helper()
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Shutdown(context.Background()) })
}

// consume builds a runtime over b whose failures go to f, and starts a
// consumer of sub over it whose handler fails each delivery with the error
// fail gives for its attempt, sending the attempt on the channel returned.
func consume(t *testing.T, b messaging.Broker, f *failures, sub messaging.Subscription, fail func(attempt int) error) <-chan int {
	t.Helper()
	ob, in := engines(t)
	rt, err := messaging.New(config(t), b, ob, in, time.Second, slog.New(slog.DiscardHandler), messaging.OnFailure(f.hook))
	if err != nil {
		t.Fatal(err)
	}
	attempts := make(chan int, 16)
	r, err := rt.Consume(sub, func(ctx context.Context, _ payload, _ messaging.Claim) error {
		attempt := messaging.Attempt(ctx)
		attempts <- attempt
		return fail(attempt)
	})
	if err != nil {
		t.Fatal(err)
	}
	start(t, r)
	return attempts
}

// event1 is the event the consumer tests publish.
var event1 = event.Event{ID: "e1", Source: "/test", Type: "t", DataContentType: "application/json", Data: []byte(`{"n":1}`)}

// awaitAttempts fails the test unless the handler is called on exactly the
// attempts want, in order, and then no more within a short wait.
func awaitAttempts(t *testing.T, attempts <-chan int, want ...int) {
	t.Helper()
	var got []int
	for len(got) < len(want) {
		select {
		case a := <-attempts:
			got = append(got, a)
		case <-time.After(failsafe):
			t.Fatalf("handler called on attempts %v, want %v", got, want)
		}
	}
	select {
	case a := <-attempts:
		got = append(got, a)
	case <-time.After(50 * time.Millisecond):
	}
	if !slices.Equal(got, want) {
		t.Fatalf("handler called on attempts %v, want %v", got, want)
	}
}

// An event whose handler fails every delivery is reported once, as
// exhausted, on the last delivery the subscription allows.
func TestExhaustionIsReportedOnceOnTheLastAttempt(t *testing.T) {
	b := memory.New()
	var f failures
	cause := errors.New("still failing")
	sub := messaging.Subscription{Name: "consumer", MaxDeliver: 3, RetryDelay: time.Millisecond}
	attempts := consume(t, b, &f, sub, func(int) error { return cause })
	if err := b.Publish(t.Context(), event1); err != nil {
		t.Fatal(err)
	}
	awaitAttempts(t, attempts, 1, 2, 3)

	got := f.all()
	if len(got) != 1 {
		t.Fatalf("hook got %+v, want one exhausted failure", got)
	}
	want := messaging.Failure{
		Kind: messaging.FailureExhausted, Consumer: "consumer",
		EventSource: "/test", EventID: "e1", EventType: "t", Attempt: 3, Err: cause,
	}
	if got[0] != want {
		t.Errorf("hook got %+v, want %+v", got[0], want)
	}
}

// A permanent refusal on the last delivery is the handler's own verdict,
// which the refusal's log line reports, so it is not an exhaustion.
func TestPermanentRefusalOnTheLastAttemptIsNotExhaustion(t *testing.T) {
	b := memory.New()
	var f failures
	sub := messaging.Subscription{Name: "consumer", MaxDeliver: 2, RetryDelay: time.Millisecond}
	attempts := consume(t, b, &f, sub, func(attempt int) error {
		if attempt == 2 {
			return event.Permanent(errors.New("refused"))
		}
		return errors.New("transient")
	})
	if err := b.Publish(t.Context(), event1); err != nil {
		t.Fatal(err)
	}
	awaitAttempts(t, attempts, 1, 2)
	if got := f.all(); len(got) != 0 {
		t.Errorf("hook got %+v, want nothing", got)
	}
}

// fakeBroker is a broker whose Subscribe returns src and keeps the report
// it was given, and whose Publish fails each event fail names, recording
// each publish context's deadline.
type fakeBroker struct {
	src  reactor.Source[event.Event]
	fail map[string]error

	mu        sync.Mutex
	report    messaging.Report
	deadlines []time.Time
}

func (b *fakeBroker) Subscribe(_ messaging.Subscription, report messaging.Report) (reactor.Source[event.Event], error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.report = report
	return b.src, nil
}

func (b *fakeBroker) Publish(ctx context.Context, e event.Event) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	d, _ := ctx.Deadline()
	b.deadlines = append(b.deadlines, d)
	return b.fail[e.ID]
}

// source delivers events once each on a context with no attempt, then
// waits for ctx to end.
type source struct {
	events []event.Event
}

func (s *source) Receive(ctx context.Context, fn reactor.Func[event.Event]) error {
	for _, e := range s.events {
		_ = fn(ctx, e)
	}
	<-ctx.Done()
	return nil
}

func (s *source) Ready() bool { return true }

// The errors a consumer's source survives reach the hook as source
// failures under the consumer's name.
func TestSourceFailuresReachTheHook(t *testing.T) {
	b := &fakeBroker{src: &source{}}
	var f failures
	consume(t, b, &f, messaging.Subscription{Name: "consumer"}, func(int) error { return nil })
	cause := errors.New("message 4 does not decode")
	b.mu.Lock()
	report := b.report
	b.mu.Unlock()
	report(t.Context(), cause)

	want := []messaging.Failure{{Kind: messaging.FailureSource, Consumer: "consumer", Err: cause}}
	if got := f.all(); !slices.Equal(got, want) {
		t.Errorf("hook got %+v, want %+v", got, want)
	}
}

// A provider that gives no attempt never exhausts an unlimited
// subscription: Attempt's 0 is not a MaxDeliver of 0.
func TestNoAttemptIsNoExhaustion(t *testing.T) {
	b := &fakeBroker{src: &source{events: []event.Event{event1}}}
	var f failures
	attempts := consume(t, b, &f, messaging.Subscription{Name: "consumer"}, func(int) error { return errors.New("fails") })
	awaitAttempts(t, attempts, 0)
	if got := f.all(); len(got) != 0 {
		t.Errorf("hook got %+v, want nothing", got)
	}
}

// Without a hook, each failure is logged at warn on the runtime's logger,
// with its kind, consumer, event, attempt, and error.
func TestDefaultHookLogsEachFailureAtWarn(t *testing.T) {
	ob, in := engines(t)
	b := memory.New()
	var log syncBuffer
	rt, err := messaging.New(config(t), b, ob, in, time.Second, slog.New(slog.NewTextHandler(&log, nil)))
	if err != nil {
		t.Fatal(err)
	}
	r, err := rt.Consume(messaging.Subscription{Name: "consumer", MaxDeliver: 1}, func(context.Context, payload, messaging.Claim) error {
		return errors.New("gave up")
	})
	if err != nil {
		t.Fatal(err)
	}
	start(t, r)
	if err := b.Publish(t.Context(), event1); err != nil {
		t.Fatal(err)
	}
	const want = `level=WARN msg="messaging failure" kind=exhausted consumer=consumer type=t event=e1 source=/test attempt=1 error="gave up"`
	waitFor(t, "the failure's log line", func() bool { return strings.Contains(log.String(), "messaging failure") })
	if !strings.Contains(log.String(), want) {
		t.Errorf("the log lacks %s:\n%s", want, log.String())
	}
}

var claimColumns = []string{"seq", "header", "data"}

// claimEvent scripts ClaimRow returning e at seq, encoded as the sink
// writes it.
func claimEvent(t *testing.T, seq int64, e event.Event) sqltest.Response {
	t.Helper()
	h, data, err := event.Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	header, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	return sqltest.Response{Columns: claimColumns, Rows: [][]driver.Value{{seq, header, data}}}
}

// The relay's reports reach the hook classified: a quarantined row as a
// quarantine, and a pass a transient publish failure or a database error
// ends as a relay failure, each with the event's identity when the relay
// knew it. Each publish is bounded by the configured relay timeout.
func TestRelayFailuresReachTheHookClassified(t *testing.T) {
	ob, in := engines(t)
	broken := errors.New("connection reset")
	down := errors.New("broker down")
	pool, rec := sqltest.Open(t,
		// The first pass: a row that does not decode, quarantined, then a
		// row whose publish fails transiently, which ends the pass.
		sqltest.Response{Columns: claimColumns, Rows: [][]driver.Value{{int64(1), []byte("not json"), []byte("{}")}}},
		sqltest.Response{Affected: 1},
		claimEvent(t, 2, event1),
		// The last pass, on shutdown: the claim fails.
		sqltest.Response{Err: broken},
	)
	db := sqlate.Wrap(pool, sqltest.Dialect{})
	cfg := config(t)
	cfg.RelayPoll = libconfig.Duration(time.Hour)
	cfg.RelayTimeout = libconfig.Duration(time.Hour)
	b := &fakeBroker{fail: map[string]error{"e1": down}}
	var f failures
	rt, err := messaging.New(cfg, b, ob, in, time.Second, slog.New(slog.DiscardHandler), messaging.OnFailure(f.hook))
	if err != nil {
		t.Fatal(err)
	}
	r := rt.Relay(db)
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the first pass's failures", func() bool { return len(f.all()) == 2 })
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rec.Pending() != 0 {
		t.Fatalf("%d scripted responses left: the last pass did not run", rec.Pending())
	}

	got := f.all()
	if len(got) != 3 {
		t.Fatalf("hook got %+v, want a quarantine and two relay failures", got)
	}
	if q, ok := errors.AsType[*outbox.QuarantineError](got[0].Err); got[0].Kind != messaging.FailureQuarantine || !ok || q.Seq != 1 || got[0].EventID != "" {
		t.Errorf("first failure = %+v, want row 1's quarantine, with no event", got[0])
	}
	if p, ok := errors.AsType[*outbox.PublishError](got[1].Err); got[1].Kind != messaging.FailureRelay || !ok || p.Seq != 2 || !errors.Is(got[1].Err, down) {
		t.Errorf("second failure = %+v, want row 2's publish failure", got[1])
	}
	if got[1].EventSource != "/test" || got[1].EventID != "e1" || got[1].EventType != "t" {
		t.Errorf("second failure = %+v, want event e1's identity", got[1])
	}
	if got[2].Kind != messaging.FailureRelay || !errors.Is(got[2].Err, broken) || got[2].EventID != "" {
		t.Errorf("third failure = %+v, want the database error, with no event", got[2])
	}
	for _, fl := range got {
		if fl.Consumer != "" || fl.Attempt != 0 {
			t.Errorf("relay failure %+v names a consumer or an attempt", fl)
		}
	}

	b.mu.Lock()
	deadlines := slices.Clone(b.deadlines)
	b.mu.Unlock()
	if len(deadlines) != 1 || time.Until(deadlines[0]) < 30*time.Minute {
		t.Errorf("publish deadlines = %v, want one an hour out, the configured relay timeout", deadlines)
	}
}

// A relay row that decodes and is quarantined, its publish refused
// permanently, carries the event's identity.
func TestRelayQuarantineCarriesTheEvent(t *testing.T) {
	ob, in := engines(t)
	pool, _ := sqltest.Open(t, claimEvent(t, 1, event1), sqltest.Response{Affected: 1}, sqltest.Response{Columns: claimColumns})
	cfg := config(t)
	cfg.RelayPoll = libconfig.Duration(time.Hour)
	b := &fakeBroker{fail: map[string]error{"e1": event.Permanent(errors.New("too large"))}}
	var f failures
	rt, err := messaging.New(cfg, b, ob, in, time.Second, slog.New(slog.DiscardHandler), messaging.OnFailure(f.hook))
	if err != nil {
		t.Fatal(err)
	}
	r := rt.Relay(sqlate.Wrap(pool, sqltest.Dialect{}))
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the quarantine", func() bool { return len(f.all()) > 0 })
	_ = r.Shutdown(context.Background())

	got := f.all()[0]
	if got.Kind != messaging.FailureQuarantine || got.EventSource != "/test" || got.EventID != "e1" || got.EventType != "t" {
		t.Errorf("failure = %+v, want e1's quarantine", got)
	}
}

func TestConfigFinalize(t *testing.T) {
	c := messaging.Config{Source: "/file"}
	c.Merge(&messaging.Config{RelayPoll: libconfig.Duration(time.Second), RelayTimeout: libconfig.Duration(time.Minute)})
	if c.RelayTimeout.Duration() != time.Minute {
		t.Errorf("Merge left RelayTimeout %v, want 1m", c.RelayTimeout)
	}
	t.Setenv("SVC_MESSAGING_SOURCE", "/env")
	t.Setenv("SVC_MESSAGING_RELAY_TIMEOUT", "3s")
	if err := c.Finalize("svc"); err != nil {
		t.Fatal(err)
	}
	if c.Source != "/env" || c.RelayPoll.Duration() != time.Second || c.RelayTimeout.Duration() != 3*time.Second {
		t.Errorf("Finalize = %+v", c)
	}

	var empty messaging.Config
	if err := empty.Finalize(""); err == nil {
		t.Error("Finalize accepted a block with no source")
	}
	if empty.RelayPoll.Duration() != messaging.DefaultRelayPoll {
		t.Errorf("RelayPoll = %v, want the default", empty.RelayPoll)
	}
	if empty.RelayTimeout.Duration() != messaging.DefaultRelayTimeout {
		t.Errorf("RelayTimeout = %v, want the default", empty.RelayTimeout)
	}

	for _, v := range []string{"0s", "-1s", "soon"} {
		t.Setenv("SVC_MESSAGING_RELAY_TIMEOUT", v)
		c := messaging.Config{Source: "/file"}
		if err := c.Finalize("svc"); err == nil {
			t.Errorf("Finalize accepted a relay timeout of %q", v)
		}
	}
}

// syncBuffer serializes the reactor's writes and the test's read.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
