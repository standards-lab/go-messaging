package outbox_test

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-messaging/outbox"
	"github.com/standards-lab/sqlate/sqltest"
)

// The relay waits this long between passes in tests that run one pass, so
// no second pass starts before the test stops it.
const noPoll = time.Hour

var claimColumns = []string{"seq", "header", "data"}

// claim scripts ClaimRow's response: the row seq holding header and data.
func claim(seq int64, header, data []byte) sqltest.Response {
	return sqltest.Response{Columns: claimColumns, Rows: [][]driver.Value{{seq, header, data}}}
}

// claimEvent scripts ClaimRow returning e, encoded as the sink writes it.
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
	return claim(seq, header, data)
}

// Scripted responses beyond the claims.
var (
	noRow    = sqltest.Response{Columns: claimColumns}
	affected = sqltest.Response{Affected: 1}
)

// reports collects what a relay reports.
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

// publisher records the events a relay hands it, failing those fail names.
type publisher struct {
	mu   sync.Mutex
	got  []string
	fail map[string]error
}

func (p *publisher) publish(_ context.Context, e event.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.got = append(p.got, e.ID)
	return p.fail[e.ID]
}

func (p *publisher) published() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.got)
}

// waitFor fails the test unless cond holds within a few seconds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// runPass runs r until its first pass has consumed every scripted response
// and rolled back its last transaction, then stops it and waits for Receive
// to return, so everything the pass reported has been reported.
func runPass(t *testing.T, r *outbox.Relay, rec *sqltest.Recorder, fn func(context.Context, event.Event) error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Receive(ctx, fn) }()
	waitFor(t, "the pass to end", func() bool {
		ops := rec.Ops()
		return rec.Pending() == 0 && len(ops) > 0 && ops[len(ops)-1] == sqltest.OpRollback
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Receive = %v, want nil", err)
	}
}

// execArgs returns the arguments of each exec whose text contains part.
func execArgs(rec *sqltest.Recorder, part string) [][]any {
	var out [][]any
	for _, c := range rec.Calls() {
		if c.Op == sqltest.OpExec && strings.Contains(c.SQL, part) {
			out = append(out, c.Args)
		}
	}
	return out
}

func TestRelayPublishesInSeqOrderOneRowPerTransaction(t *testing.T) {
	db, rec := open(t,
		claimEvent(t, 1, testEvent("a")), affected,
		claimEvent(t, 2, testEvent("b")), affected,
		noRow,
	)
	var rep reports
	var pub publisher
	runPass(t, newOutbox(t).Relay(db, noPoll, rep.report), rec, pub.publish)

	if got := pub.published(); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("published %v, want [a b]", got)
	}
	row := []sqltest.Op{sqltest.OpBegin, sqltest.OpQuery, sqltest.OpExec, sqltest.OpCommit}
	want := slices.Concat(row, row, []sqltest.Op{sqltest.OpBegin, sqltest.OpQuery, sqltest.OpRollback})
	if ops := rec.Ops(); !slices.Equal(ops, want) {
		t.Errorf("ops = %v, want %v: each row claimed and marked in its own transaction", ops, want)
	}
	marks := execArgs(rec, "SET p = 1")
	if len(marks) != 2 || marks[0][0] != int64(1) || marks[1][0] != int64(2) {
		t.Errorf("marked %v, want seq 1 then 2", marks)
	}
	if errs := rep.all(); len(errs) != 0 {
		t.Errorf("reported %v on a clean pass", errs)
	}
}

func TestCorruptRowIsQuarantinedAndTheNextRowPublishes(t *testing.T) {
	// A header that is not JSON, and one that is but holds no event.
	undecodable, err := json.Marshal(event.Header{"ce-id": {"x"}})
	if err != nil {
		t.Fatal(err)
	}
	for name, header := range map[string][]byte{
		"header": []byte("not json"),
		"event":  undecodable,
	} {
		t.Run(name, func(t *testing.T) {
			db, rec := open(t,
				claim(1, header, []byte("{}")), affected,
				claimEvent(t, 2, testEvent("b")), affected,
				noRow,
			)
			var rep reports
			var pub publisher
			runPass(t, newOutbox(t).Relay(db, noPoll, rep.report), rec, pub.publish)

			if got := pub.published(); !slices.Equal(got, []string{"b"}) {
				t.Errorf("published %v, want [b]: the corrupt row is never handed to the handler", got)
			}
			q := execArgs(rec, "SET q")
			// Quarantine binds reason, then seq, in the statement's order.
			if len(q) != 1 || q[0][1] != int64(1) {
				t.Fatalf("quarantined %v, want row 1", q)
			}
			if reason, _ := q[0][0].(string); !strings.Contains(reason, name+" does not decode") {
				t.Errorf("reason = %q, want it to say the %s does not decode", reason, name)
			}
			row := []sqltest.Op{sqltest.OpBegin, sqltest.OpQuery, sqltest.OpExec, sqltest.OpCommit}
			want := slices.Concat(row, row, []sqltest.Op{sqltest.OpBegin, sqltest.OpQuery, sqltest.OpRollback})
			if ops := rec.Ops(); !slices.Equal(ops, want) {
				t.Errorf("ops = %v, want %v: the quarantine commits in the row's own transaction", ops, want)
			}
			errs := rep.all()
			if len(errs) != 1 {
				t.Fatalf("reported %v, want the one quarantine", errs)
			}
			var qe *outbox.QuarantineError
			if !errors.As(errs[0], &qe) || qe.Seq != 1 || qe.Event.ID != "" || qe.Reason != q[0][0] {
				t.Errorf("report = %#v, want row 1's quarantine, with no event and the reason recorded", errs[0])
			}
		})
	}
}

func TestPermanentPublishFailureIsQuarantinedAndTheNextRowPublishes(t *testing.T) {
	cause := errors.New("rejected by the broker")
	db, rec := open(t,
		claimEvent(t, 1, testEvent("a")), affected,
		claimEvent(t, 2, testEvent("b")), affected,
		noRow,
	)
	var rep reports
	pub := publisher{fail: map[string]error{"a": event.Permanent(cause)}}
	runPass(t, newOutbox(t).Relay(db, noPoll, rep.report), rec, pub.publish)

	if got := pub.published(); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("published %v, want [a b]: the pass goes on past the quarantined row", got)
	}
	q := execArgs(rec, "SET q")
	if len(q) != 1 || q[0][1] != int64(1) {
		t.Fatalf("quarantined %v, want row 1", q)
	}
	if reason, _ := q[0][0].(string); !strings.Contains(reason, cause.Error()) {
		t.Errorf("reason = %q, want it to carry the publish error", reason)
	}
	if marks := execArgs(rec, "SET p = 1"); len(marks) != 1 || marks[0][0] != int64(2) {
		t.Errorf("marked %v, want only row 2 published", marks)
	}
	errs := rep.all()
	if len(errs) != 1 {
		t.Fatalf("reported %v, want the one quarantine", errs)
	}
	var qe *outbox.QuarantineError
	if !errors.As(errs[0], &qe) || qe.Seq != 1 || qe.Event.ID != "a" {
		t.Fatalf("report = %#v, want row 1's quarantine with its event", errs[0])
	}
	if !errors.Is(qe, cause) || !event.IsPermanent(qe) {
		t.Errorf("quarantine %v does not carry the handler's permanent error", qe)
	}
}

func TestTransientPublishFailureEndsThePassAndIsReported(t *testing.T) {
	cause := errors.New("broker down")
	db, rec := open(t, claimEvent(t, 1, testEvent("a")))
	var rep reports
	pub := publisher{fail: map[string]error{"a": cause}}
	r := newOutbox(t).Relay(db, noPoll, rep.report)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Receive(ctx, pub.publish) }()
	waitFor(t, "the report", func() bool { return len(rep.all()) > 0 })
	if !r.Ready() {
		t.Error("a publish failure made the relay not ready; readiness speaks only for the database")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Receive = %v, want nil", err)
	}

	want := []sqltest.Op{sqltest.OpBegin, sqltest.OpQuery, sqltest.OpRollback}
	if ops := rec.Ops(); !slices.Equal(ops, want) {
		t.Errorf("ops = %v, want %v: the row is neither marked nor quarantined, and the pass ends", ops, want)
	}
	errs := rep.all()
	if len(errs) != 1 {
		t.Fatalf("reported %v, want the one publish failure", errs)
	}
	var pe *outbox.PublishError
	if !errors.As(errs[0], &pe) || pe.Seq != 1 || pe.Event.ID != "a" || !errors.Is(pe, cause) {
		t.Errorf("report = %#v, want row 1's publish error", errs[0])
	}
	if _, ok := errors.AsType[*outbox.QuarantineError](errs[0]); ok {
		t.Error("a transient failure was reported as a quarantine")
	}
}

func TestQuarantineFailureEndsThePass(t *testing.T) {
	broken := errors.New("connection reset")
	db, rec := open(t, claim(1, []byte("not json"), nil), sqltest.Response{Err: broken})
	var rep reports
	var pub publisher
	r := newOutbox(t).Relay(db, noPoll, rep.report)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Receive(ctx, pub.publish) }()
	waitFor(t, "the relay to become not ready", func() bool { return !r.Ready() && len(rep.all()) > 0 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Receive = %v, want nil", err)
	}

	want := []sqltest.Op{sqltest.OpBegin, sqltest.OpQuery, sqltest.OpExec, sqltest.OpRollback}
	if ops := rec.Ops(); !slices.Equal(ops, want) {
		t.Errorf("ops = %v, want %v: the pass ends on the failed quarantine", ops, want)
	}
	errs := rep.all()
	if len(errs) != 1 || !errors.Is(errs[0], broken) {
		t.Fatalf("reported %v, want the database error", errs)
	}
	if _, ok := errors.AsType[*outbox.QuarantineError](errs[0]); ok {
		t.Error("a quarantine that failed was reported as made")
	}
}

func TestDatabaseErrorIsReportedAndMakesTheRelayNotReadyUntilAPassSucceeds(t *testing.T) {
	broken := errors.New("connection reset")
	db, rec := open(t, sqltest.Response{Err: broken})
	var rep reports
	var pub publisher
	r := newOutbox(t).Relay(db, time.Millisecond, rep.report)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Receive(ctx, pub.publish) }()
	waitFor(t, "the report", func() bool { return len(rep.all()) > 0 })
	waitFor(t, "the relay to become not ready", func() bool { return !r.Ready() })
	errs := rep.all()
	if len(errs) == 0 || !errors.Is(errs[0], broken) {
		t.Fatalf("reported %v, want the database error first", errs)
	}
	// Every later pass finds the outbox empty.
	for range 10000 {
		rec.Queue(noRow)
	}
	waitFor(t, "the relay to become ready again", r.Ready)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Receive = %v, want nil", err)
	}
}

func TestMarkThatChangesNoRowFailsThePass(t *testing.T) {
	db, rec := open(t, claimEvent(t, 1, testEvent("a")), sqltest.Response{Affected: 0})
	var rep reports
	var pub publisher
	runPass(t, newOutbox(t).Relay(db, noPoll, rep.report), rec, pub.publish)

	errs := rep.all()
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "changed 0 rows") {
		t.Errorf("reported %v, want the mark that changed no row", errs)
	}
	if ops := rec.Ops(); slices.Contains(ops, sqltest.OpCommit) {
		t.Errorf("ops = %v: a mark that changed no row was committed", ops)
	}
}

func TestQuarantineThatChangesNoRowFailsThePass(t *testing.T) {
	db, rec := open(t, claim(1, []byte("not json"), nil), sqltest.Response{Affected: 0})
	var rep reports
	var pub publisher
	runPass(t, newOutbox(t).Relay(db, noPoll, rep.report), rec, pub.publish)

	errs := rep.all()
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "changed 0 rows") {
		t.Errorf("reported %v, want the quarantine that changed no row", errs)
	}
	if ops := rec.Ops(); slices.Contains(ops, sqltest.OpCommit) {
		t.Errorf("ops = %v: a quarantine that changed no row was committed", ops)
	}
}

func TestTimeoutEndsAHungHandler(t *testing.T) {
	db, rec := open(t, claimEvent(t, 1, testEvent("a")))
	var rep reports
	r := newOutbox(t).Relay(db, noPoll, rep.report, outbox.Timeout(20*time.Millisecond))
	hung := func(ctx context.Context, _ event.Event) error {
		<-ctx.Done()
		return ctx.Err()
	}
	runPass(t, r, rec, hung)

	errs := rep.all()
	if len(errs) != 1 || !errors.Is(errs[0], context.DeadlineExceeded) {
		t.Fatalf("reported %v, want the handler's deadline", errs)
	}
	if _, ok := errors.AsType[*outbox.PublishError](errs[0]); !ok {
		t.Errorf("report = %#v, want a transient publish error: a timeout is not permanent", errs[0])
	}
}

func TestStopSettlesTheRowInFlight(t *testing.T) {
	db, rec := open(t, claimEvent(t, 1, testEvent("a")), affected)
	var rep reports
	r := newOutbox(t).Relay(db, noPoll, rep.report)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	handler := func(hctx context.Context, _ event.Event) error {
		close(started)
		<-ctx.Done()
		// The stop does not reach the handler of a claimed row.
		return hctx.Err()
	}
	done := make(chan error, 1)
	go func() { done <- r.Receive(ctx, handler) }()
	<-started
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Receive = %v, want nil", err)
	}
	want := []sqltest.Op{sqltest.OpBegin, sqltest.OpQuery, sqltest.OpExec, sqltest.OpCommit}
	if ops := rec.Ops(); !slices.Equal(ops, want) {
		t.Errorf("ops = %v, want %v: the row in flight is marked and committed", ops, want)
	}
	if errs := rep.all(); len(errs) != 0 {
		t.Errorf("reported %v; the stop is not a failure", errs)
	}
}

func TestDrainPublishesWhatWasCommittedBeforeTheStop(t *testing.T) {
	db, rec := open(t,
		claimEvent(t, 1, testEvent("a")), affected,
		claim(2, []byte("not json"), nil), affected,
		noRow,
	)
	var rep reports
	var pub publisher
	r := newOutbox(t).Relay(db, noPoll, rep.report, outbox.Drain(time.Second))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Receive(ctx, pub.publish); err != nil {
		t.Fatalf("Receive = %v, want nil", err)
	}
	if got := pub.published(); !slices.Equal(got, []string{"a"}) {
		t.Errorf("published %v, want [a] in the last pass", got)
	}
	if rec.Pending() != 0 {
		t.Errorf("%d scripted responses left: the last pass stopped early", rec.Pending())
	}
	var qe *outbox.QuarantineError
	if errs := rep.all(); len(errs) != 1 || !errors.As(errs[0], &qe) {
		t.Errorf("reported %v, want the last pass's quarantine", errs)
	}
}

func TestNoDrainMakesNoLastPass(t *testing.T) {
	db, rec := open(t)
	var pub publisher
	r := newOutbox(t).Relay(db, noPoll, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Receive(ctx, pub.publish); err != nil {
		t.Fatalf("Receive = %v, want nil", err)
	}
	if ops := rec.Ops(); len(ops) != 0 {
		t.Errorf("ops = %v, want none without a Drain", ops)
	}
}

func TestReportGetsTheReceivingContext(t *testing.T) {
	type key struct{}
	db, rec := open(t, claim(1, []byte("not json"), nil), affected, noRow)
	var got []any
	var mu sync.Mutex
	report := func(ctx context.Context, _ error) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, ctx.Value(key{}))
	}
	var pub publisher
	r := newOutbox(t).Relay(db, noPoll, report)
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "receiving"))
	done := make(chan error, 1)
	go func() { done <- r.Receive(ctx, pub.publish) }()
	waitFor(t, "the pass to end", func() bool { return rec.Pending() == 0 && len(rec.Ops()) == 7 })
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0] != "receiving" {
		t.Errorf("report contexts carried %v, want the receiving context's value", got)
	}
}

func TestNilReportDiscards(t *testing.T) {
	db, rec := open(t, claim(1, []byte("not json"), nil), affected, noRow)
	var pub publisher
	runPass(t, newOutbox(t).Relay(db, noPoll, nil), rec, pub.publish)
	if q := execArgs(rec, "SET q"); len(q) != 1 {
		t.Errorf("quarantined %v, want row 1 without a report function", q)
	}
}

func TestReadyFromTheStartOfReceive(t *testing.T) {
	db, _ := open(t, noRow)
	r := newOutbox(t).Relay(db, noPoll, nil)
	if r.Ready() {
		t.Error("ready before Receive")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	var pub publisher
	go func() { done <- r.Receive(ctx, pub.publish) }()
	waitFor(t, "the relay to be ready", r.Ready)
	cancel()
	<-done
	if r.Ready() {
		t.Error("ready after Receive returned")
	}
}

func TestRelayRefusesABadOption(t *testing.T) {
	db, _ := open(t)
	ob := newOutbox(t)
	for name, build := range map[string]func(){
		"poll":    func() { ob.Relay(db, 0, nil) },
		"timeout": func() { ob.Relay(db, time.Second, nil, outbox.Timeout(0)) },
		"drain":   func() { ob.Relay(db, time.Second, nil, outbox.Drain(-1)) },
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("Relay accepted a bad %s", name)
				}
			}()
			build()
		})
	}
}
