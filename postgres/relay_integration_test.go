//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-core/reactor"
	"github.com/standards-lab/go-messaging"
	"github.com/standards-lab/go-messaging/memory"
	"github.com/standards-lab/go-messaging/outbox"
	"github.com/standards-lab/go-messaging/postgres"
	"github.com/standards-lab/sqlate"
	pgdialect "github.com/standards-lab/sqlate/postgres"
	"github.com/standards-lab/sqlate/query"
)

const (
	poll     = 20 * time.Millisecond
	failsafe = 10 * time.Second
)

type recorder struct {
	mu  sync.Mutex
	ids []string
}

func (r *recorder) add(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ids = append(r.ids, id)
}

func (r *recorder) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.ids)
}

func (r *recorder) record(_ context.Context, e event.Event) error {
	r.add(e.ID)
	return nil
}

func relay(t *testing.T, db *sqlate.DB, fn reactor.Func[event.Event], opts reactor.Options) *reactor.Reactor[event.Event] {
	t.Helper()
	r := reactor.New(ob.Relay(db, poll, outbox.RelayOptions{}), fn, opts)
	if err := r.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stop(r) })
	return r
}

func observe(t *testing.T, b *memory.Broker) *recorder {
	t.Helper()
	src, err := b.Subscribe(messaging.Subscription{Name: "observer"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	r := reactor.New(src, rec.record, reactor.Options{})
	if err := r.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stop(r) })
	return rec
}

func stop(r *reactor.Reactor[event.Event]) {
	ctx, cancel := context.WithTimeout(context.Background(), failsafe)
	defer cancel()
	_ = r.Shutdown(ctx)
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

func ids(from, to int) []string {
	var out []string
	for n := from; n <= to; n++ {
		out = append(out, strconv.Itoa(n))
	}
	return out
}

func emitEach(t *testing.T, db *sqlate.DB, idList ...string) {
	t.Helper()
	for _, id := range idList {
		emit(t, db, tick(id))
	}
}

func drained(t *testing.T, db *sqlate.DB) func() bool {
	return func() bool {
		_, pending := rows(t, db)
		return pending == 0
	}
}

func TestRelayPublishesInEmissionOrder(t *testing.T) {
	db := migrated(t)
	emitEach(t, db, ids(1, 5)...)
	rec := &recorder{}
	r := relay(t, db, rec.record, reactor.Options{})
	eventually(t, "five events", func() bool { return len(rec.seen()) == 5 })
	if got := rec.seen(); !slices.Equal(got, ids(1, 5)) {
		t.Fatalf("published %v, want %v", got, ids(1, 5))
	}
	if !r.Ready() {
		t.Error("a running relay is not ready")
	}
	eventually(t, "no pending rows", drained(t, db))
}

func TestStopBetweenCommitAndPublishLosesNoEvent(t *testing.T) {
	db := migrated(t)
	b := memory.New()
	rec := observe(t, b)

	emitEach(t, db, "1", "2", "3")
	entered := make(chan struct{})
	first := relay(t, db, func(ctx context.Context, e event.Event) error {
		if e.ID == "3" {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}
		return b.Publish(ctx, e)
	}, reactor.Options{CancelAfter: 50 * time.Millisecond})
	<-entered
	err := first.Shutdown(t.Context())
	if err == nil || !strings.Contains(err.Error(), "handlers cancelled") {
		t.Fatalf("first relay's shutdown = %v, want its handler cancelled", err)
	}
	if _, pending := rows(t, db); pending != 1 {
		t.Fatalf("after the stop, %d rows pending, want 1 (event 3)", pending)
	}

	emitEach(t, db, "4")
	relay(t, db, b.Publish, reactor.Options{})
	eventually(t, "four deliveries", func() bool { return len(rec.seen()) == 4 })
	if got := rec.seen(); !slices.Equal(got, ids(1, 4)) {
		t.Fatalf("delivered %v, want %v", got, ids(1, 4))
	}
	eventually(t, "no pending rows", drained(t, db))
}

func TestFailedPublishIsRetriedInOrder(t *testing.T) {
	db := migrated(t)
	emitEach(t, db, "1", "2")
	rec := &recorder{}
	var once sync.Once
	relay(t, db, func(ctx context.Context, e event.Event) error {
		rec.add(e.ID)
		failed := false
		if e.ID == "1" {
			once.Do(func() { failed = true })
		}
		if failed {
			return errors.New("broker unavailable")
		}
		return nil
	}, reactor.Options{})
	eventually(t, "three attempts", func() bool { return len(rec.seen()) == 3 })
	if got, want := rec.seen(), []string{"1", "1", "2"}; !slices.Equal(got, want) {
		t.Fatalf("attempts %v, want %v: a failed row is retried before the rows after it", got, want)
	}
	eventually(t, "no pending rows", drained(t, db))
}

func TestPublishedButUnmarkedIsRepublished(t *testing.T) {
	db := migrated(t)
	b := memory.New()
	rec := observe(t, b)
	emitEach(t, db, "1")
	var once sync.Once
	published := &recorder{}
	relay(t, db, func(ctx context.Context, e event.Event) error {
		if err := b.Publish(ctx, e); err != nil {
			return err
		}
		published.add(e.ID)
		var err error
		once.Do(func() { err = errors.New("stopped before the mark") })
		return err
	}, reactor.Options{})
	eventually(t, "no pending rows", drained(t, db))
	if got := published.seen(); !slices.Equal(got, []string{"1", "1"}) {
		t.Fatalf("published %v, want event 1 twice", got)
	}
	time.Sleep(10 * poll)
	if got := rec.seen(); !slices.Equal(got, []string{"1"}) {
		t.Fatalf("delivered %v, want event 1 once", got)
	}
}

func TestRelaysPublishEachRowOnce(t *testing.T) {
	db := migrated(t)
	const n = 40
	emitEach(t, db, ids(1, n)...)
	rec := &recorder{}
	slow := func(ctx context.Context, e event.Event) error {
		time.Sleep(2 * time.Millisecond)
		return rec.record(ctx, e)
	}
	relay(t, db, slow, reactor.Options{})
	relay(t, db, slow, reactor.Options{})
	eventually(t, "no pending rows", drained(t, db))
	got := rec.seen()
	slices.SortFunc(got, func(a, b string) int {
		x, _ := strconv.Atoi(a)
		y, _ := strconv.Atoi(b)
		return x - y
	})
	if !slices.Equal(got, ids(1, n)) {
		t.Fatalf("published %d events %v, want each of 1..%d once", len(got), got, n)
	}
}

type watched struct {
	reactor.Source[event.Event]
	stopped chan struct{}
}

func (w watched) Receive(ctx context.Context, fn reactor.Func[event.Event]) error {
	context.AfterFunc(ctx, func() { close(w.stopped) })
	return w.Source.Receive(ctx, fn)
}

func TestShutdownSettlesTheRowInFlight(t *testing.T) {
	db := migrated(t)
	emitEach(t, db, "1")
	entered, release := make(chan struct{}), make(chan struct{})
	src := watched{ob.Relay(db, poll, outbox.RelayOptions{}), make(chan struct{})}
	r := reactor.New[event.Event](src, func(context.Context, event.Event) error {
		close(entered)
		<-release
		return nil
	}, reactor.Options{})
	if err := r.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stop(r) })
	<-entered
	done := make(chan error, 1)
	go func() { done <- r.Shutdown(t.Context()) }()
	<-src.stopped
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if _, pending := rows(t, db); pending != 0 {
		t.Fatalf("the row in flight at shutdown is still pending")
	}
}

type reports struct {
	mu   sync.Mutex
	errs []error
}

func (r *reports) report(_ context.Context, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, err)
}

func (r *reports) all() []error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.errs)
}

func reporting(t *testing.T, db *sqlate.DB, fn reactor.Func[event.Event]) (*reactor.Reactor[event.Event], *reports) {
	t.Helper()
	rep := &reports{}
	r := reactor.New(ob.Relay(db, poll, outbox.RelayOptions{Report: rep.report}), fn, reactor.Options{})
	if err := r.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stop(r) })
	return r, rep
}

func quarantined(t *testing.T, db *sqlate.DB, id string) (at bool, reason string, published bool) {
	t.Helper()
	r, err := db.QueryContext(t.Context(),
		`SELECT quarantined_at IS NOT NULL, coalesce(quarantine_reason, ''), published_at IS NOT NULL FROM messaging_outbox WHERE id = $1`, id)
	if err != nil {
		t.Fatalf("read row %s: %v", id, err)
	}
	defer func() { _ = r.Close() }()
	if !r.Next() {
		t.Fatalf("no outbox row %s", id)
	}
	if err := r.Scan(&at, &reason, &published); err != nil {
		t.Fatalf("scan row %s: %v", id, err)
	}
	return at, reason, published
}

func quarantine(t *testing.T, db *sqlate.DB, rep *reports, id string) *outbox.QuarantineError {
	t.Helper()
	eventually(t, "the quarantine's report", func() bool { return len(rep.all()) > 0 })
	errs := rep.all()
	var q *outbox.QuarantineError
	if len(errs) != 1 || !errors.As(errs[0], &q) {
		t.Fatalf("reports = %v, want one quarantine", errs)
	}
	at, reason, published := quarantined(t, db, id)
	if !at || published || reason != q.Reason {
		t.Fatalf("row %s: quarantined %v, published %v, reason %q; want quarantined, unpublished, reason %q",
			id, at, published, reason, q.Reason)
	}
	return q
}

func TestCorruptRowIsQuarantined(t *testing.T) {
	for name, tc := range map[string]struct{ header, reason string }{
		"a header that does not decode": {`{"ce-id": "c"}`, "header does not decode: "},
		"an event that does not decode": {`{"ce-id": ["c"]}`, "event does not decode: "},
	} {
		t.Run(name, func(t *testing.T) {
			db := migrated(t)
			if _, err := db.ExecContext(t.Context(),
				`INSERT INTO messaging_outbox (source, id, header) VALUES ('/x', 'c', $1)`, tc.header); err != nil {
				t.Fatal(err)
			}
			emitEach(t, db, "1")
			rec := &recorder{}
			r, rep := reporting(t, db, rec.record)
			eventually(t, "event 1", func() bool { return slices.Equal(rec.seen(), []string{"1"}) })
			q := quarantine(t, db, rep, "c")
			if !strings.HasPrefix(q.Reason, tc.reason) || q.Err == nil {
				t.Errorf("quarantine reason %q, cause %v; want %q and its cause", q.Reason, q.Err, tc.reason)
			}
			assertNothingPending(t, db, r)
		})
	}
}

func TestPermanentFailureIsQuarantined(t *testing.T) {
	db := migrated(t)
	emitEach(t, db, "1", "2")
	rejected := errors.New("rejected by the broker")
	attempts := &recorder{}
	rec := &recorder{}
	r, rep := reporting(t, db, func(ctx context.Context, e event.Event) error {
		attempts.add(e.ID)
		if e.ID == "1" {
			return event.Permanent(rejected)
		}
		return rec.record(ctx, e)
	})
	eventually(t, "event 2", func() bool { return slices.Equal(rec.seen(), []string{"2"}) })
	q := quarantine(t, db, rep, "1")
	if q.Event.ID != "1" || !errors.Is(q, rejected) || !strings.HasPrefix(q.Reason, "publish failed permanently: ") {
		t.Errorf("quarantine of event %q, reason %q, cause %v; want event 1's permanent failure", q.Event.ID, q.Reason, q.Err)
	}
	time.Sleep(10 * poll)
	if got := attempts.seen(); !slices.Equal(got, []string{"1", "2"}) {
		t.Errorf("attempts %v, want each event once: a quarantined row is never claimed again", got)
	}
	assertNothingPending(t, db, r)
}

func assertNothingPending(t *testing.T, db *sqlate.DB, r *reactor.Reactor[event.Event]) {
	t.Helper()
	eventually(t, "no pending rows", drained(t, db))
	n, err := postgres.Pending(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("Pending = %d, want 0: a quarantined row awaits no publication", n)
	}
	if !r.Ready() {
		t.Error("a relay that quarantined a row is not ready")
	}
	select {
	case err := <-r.Err():
		t.Errorf("the relay ended: %v", err)
	default:
	}
}

func TestDatabaseErrorMakesTheRelayNotReady(t *testing.T) {
	db := migrated(t)
	rec := &recorder{}
	r := relay(t, db, rec.record, reactor.Options{})
	eventually(t, "a ready relay", r.Ready)
	if _, err := db.ExecContext(t.Context(), `ALTER TABLE messaging_outbox RENAME TO messaging_outbox_away`); err != nil {
		t.Fatal(err)
	}
	eventually(t, "a relay that is not ready", func() bool { return !r.Ready() })
	if _, err := db.ExecContext(t.Context(), `ALTER TABLE messaging_outbox_away RENAME TO messaging_outbox`); err != nil {
		t.Fatal(err)
	}
	eventually(t, "a ready relay again", r.Ready)
	emitEach(t, db, "1")
	eventually(t, "event 1", func() bool { return slices.Equal(rec.seen(), []string{"1"}) })
}

func TestTimeoutEndsAHungHandler(t *testing.T) {
	db := migrated(t)
	emitEach(t, db, "1")
	var once sync.Once
	var first error
	rec := &recorder{}
	rel := reactor.New[event.Event](ob.Relay(db, poll, outbox.RelayOptions{Timeout: 50 * time.Millisecond}),
		func(ctx context.Context, e event.Event) error {
			hung := false
			once.Do(func() { hung = true })
			if hung {
				<-ctx.Done()
				first = ctx.Err()
				return first
			}
			return rec.record(ctx, e)
		}, reactor.Options{})
	if err := rel.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stop(rel) })
	eventually(t, "event 1 on the retry", func() bool { return len(rec.seen()) == 1 })
	if !errors.Is(first, context.DeadlineExceeded) {
		t.Fatalf("the hung handler's context ended with %v, want the deadline", first)
	}
	eventually(t, "no pending rows", drained(t, db))
}

func TestTransactionBoundOutlastsAHandlerThatIgnoresItsDeadline(t *testing.T) {
	db := migrated(t)
	emitEach(t, db, "1")
	const timeout = 50 * time.Millisecond
	var once sync.Once
	rec := &recorder{}
	relay := reactor.New[event.Event](ob.Relay(db, poll, outbox.RelayOptions{Timeout: timeout}),
		func(ctx context.Context, e event.Event) error {
			rec.add(e.ID)
			once.Do(func() { time.Sleep(3 * timeout) })
			return nil
		}, reactor.Options{})
	if err := relay.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stop(relay) })
	eventually(t, "no pending rows", drained(t, db))
	if got := rec.seen(); !slices.Equal(got, []string{"1", "1"}) {
		t.Fatalf("attempts %v, want event 1 twice: the first transaction must roll back", got)
	}
}

func TestMarkThatChangesNoRowFailsThePass(t *testing.T) {
	db := migrated(t)
	emitEach(t, db, "1")
	eng := postgres.Outbox()
	eng.MarkPublished = compileMark(t, "UPDATE messaging_outbox SET published_at = now() WHERE seq = {{seq:bigint}} AND false")
	broken, err := outbox.New(eng)
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	r := reactor.New[event.Event](broken.Relay(db, poll, outbox.RelayOptions{}), rec.record, reactor.Options{})
	if err := r.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stop(r) })
	eventually(t, "two attempts", func() bool { return len(rec.seen()) >= 2 })
	eventually(t, "a relay that is not ready", func() bool { return !r.Ready() })
	if _, pending := rows(t, db); pending != 1 {
		t.Fatalf("pending = %d, want the row still pending", pending)
	}
}

func compileMark(t *testing.T, text string) query.Statement {
	t.Helper()
	fsys := fstest.MapFS{"s/mark_published.sql": &fstest.MapFile{Data: []byte("--| tier: standard\n--| transaction: required\n" + text)}}
	return query.MustCatalog(query.Patterns()).MustCompile(fsys, "s", pgdialect.Dialect{}).Statement("mark_published")
}

func TestDrainPublishesWhatWasCommittedBeforeTheStop(t *testing.T) {
	for _, tc := range []struct {
		name    string
		opts    outbox.RelayOptions
		pending int
	}{
		{"with a drain", outbox.RelayOptions{Drain: 5 * time.Second}, 0},
		{"without one", outbox.RelayOptions{}, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := migrated(t)
			b := memory.New()
			rec := observe(t, b)
			r := reactor.New[event.Event](ob.Relay(db, time.Hour, tc.opts), b.Publish, reactor.Options{CancelAfter: 10 * time.Second})
			if err := r.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			eventually(t, "the relay's first pass", r.Ready)
			time.Sleep(50 * time.Millisecond)
			emitEach(t, db, "1", "2", "3")
			if err := r.Shutdown(t.Context()); err != nil {
				t.Fatalf("shutdown: %v", err)
			}
			if _, pending := rows(t, db); pending != tc.pending {
				t.Fatalf("after shutdown, %d rows pending, want %d", pending, tc.pending)
			}
			if tc.pending == 0 {
				eventually(t, "three deliveries", func() bool { return len(rec.seen()) == 3 })
			}
		})
	}
}
