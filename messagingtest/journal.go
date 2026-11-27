package messagingtest

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-messaging"
)

type journal interface {
	messaging.Broker
	messaging.Journal
}

// RunJournal runs the journal's conformance cases, each as a subtest on a
// broker from newJournal: a [messaging.Broker] that is also the
// [messaging.Journal] on its stream, ready to append to.
func RunJournal[B interface {
	messaging.Broker
	messaging.Journal
}](t *testing.T, newJournal func(t *testing.T) B) {
	cases := []struct {
		name string
		fn   func(*testing.T, journal)
	}{
		{"AppendSequences", testAppendSequences},
		{"AppendConflicts", testAppendConflicts},
		{"AppendRetryAfterStored", testAppendRetryAfterStored},
		{"AppendStaleRepeatConflicts", testAppendStaleRepeatConflicts},
		{"AppendDeduplicates", testAppendDeduplicates},
		{"AppendSharesPublishWindow", testAppendSharesPublishWindow},
		{"AppendRejectsInvalid", testAppendRejectsInvalid},
		{"ReadInOrder", testReadInOrder},
		{"ReadEndsAtStart", testReadEndsAtStart},
		{"ReadReleasesOnBreak", testReadReleasesOnBreak},
		{"AppendReachesSubscriptions", testAppendReachesSubscriptions},
		{"AppendFilterMatchesSubject", testAppendFilterMatchesSubject},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { c.fn(t, newJournal(t)) })
	}
}

func appended(t *testing.T, j journal, subject string, expect uint64, e event.Event) uint64 {
	t.Helper()
	seq, err := j.Append(context.Background(), subject, expect, e)
	if err != nil {
		t.Fatalf("Append %s to %s at %d: %v", e.ID, subject, expect, err)
	}
	return seq
}

func testAppendSequences(t *testing.T, j journal) {
	first := appended(t, j, "journal.a", 0, ev("1", "t"))
	if first == 0 {
		t.Fatal("Append returned sequence 0")
	}
	second := appended(t, j, "journal.a", first, ev("2", "t"))
	if second <= first {
		t.Errorf("second append's sequence %d is not after the first's, %d", second, first)
	}
	other := appended(t, j, "journal.b", 0, ev("3", "t"))
	if other <= second {
		t.Errorf("another subject's first append has sequence %d, want one after %d: the stream numbers every subject", other, second)
	}
	if third := appended(t, j, "journal.a", second, ev("4", "t")); third <= other {
		t.Errorf("an append after another subject's has sequence %d, want one after %d", third, other)
	}
}

func testAppendConflicts(t *testing.T, j journal) {
	first := appended(t, j, "journal.a", 0, ev("1", "t"))
	for _, c := range []struct {
		subject string
		expect  uint64
	}{
		{"journal.a", 0},
		{"journal.a", first + 41},
		{"journal.empty", first},
	} {
		_, err := j.Append(context.Background(), c.subject, c.expect, ev("2", "t"))
		if !errors.Is(err, messaging.ErrConflict) {
			t.Errorf("Append to %s at %d: got %v, want ErrConflict", c.subject, c.expect, err)
			continue
		}
		if msg := err.Error(); !strings.Contains(msg, c.subject) || !strings.Contains(msg, strconv.FormatUint(c.expect, 10)) {
			t.Errorf("the conflict %q does not name the subject %s and the expected sequence %d", msg, c.subject, c.expect)
		}
	}
	elsewhere := ev("1", "t")
	elsewhere.Source = "/elsewhere"
	if _, err := j.Append(context.Background(), "journal.a", 0, elsewhere); !errors.Is(err, messaging.ErrConflict) {
		t.Errorf("another source's event with the last event's id: got %v, want ErrConflict", err)
	}
	if second := appended(t, j, "journal.a", first, ev("2", "t")); second <= first {
		t.Errorf("the append after the conflicts has sequence %d, want one after %d", second, first)
	}
	appended(t, j, "journal.empty", 0, ev("3", "t"))
}

func testAppendRetryAfterStored(t *testing.T, j journal) {
	first := appended(t, j, "journal.a", 0, ev("1", "t"))
	second := appended(t, j, "journal.a", first, ev("2", "t"))
	if got := appended(t, j, "journal.a", first, ev("2", "t")); got != second {
		t.Errorf("retrying the last append returned %d, want its sequence %d", got, second)
	}
	if third := appended(t, j, "journal.a", second, ev("3", "t")); third <= second {
		t.Errorf("the append after the retry has sequence %d, want one after %d: the retry must store nothing", third, second)
	}
}

func testAppendStaleRepeatConflicts(t *testing.T, j journal) {
	first := appended(t, j, "journal.a", 0, ev("1", "t"))
	second := appended(t, j, "journal.a", first, ev("2", "t"))
	if seq, err := j.Append(context.Background(), "journal.a", 0, ev("1", "t")); !errors.Is(err, messaging.ErrConflict) {
		t.Errorf("repeating an append that is no longer the subject's last, at its stale sequence: got %d, %v; want ErrConflict", seq, err)
	}
	third := appended(t, j, "journal.a", second, ev("3", "t"))
	if got, want := ids(read(t, j, "journal.a", 0)), []string{"1", "2", "3"}; !reflect.DeepEqual(got, want) {
		t.Errorf("read %v, want %v: the conflicting repeat stores nothing", got, want)
	}
	if third <= second {
		t.Errorf("the append after the repeat has sequence %d, want one after %d", third, second)
	}
}

func testAppendDeduplicates(t *testing.T, j journal) {
	e := ev("once", "t")
	first := appended(t, j, "journal.a", 0, e)
	appended(t, j, "journal.a", first, ev("next", "t"))
	if got := appended(t, j, "journal.b", 0, e); got != first {
		t.Errorf("a repeat on another subject returned %d, want the original sequence %d", got, first)
	}
	appended(t, j, "journal.b", 0, ev("fresh", "t"))
}

func testAppendSharesPublishWindow(t *testing.T, j journal) {
	published := ev("published", "journal.pub")
	publish(t, j, published)
	seq := appended(t, j, "journal.other", 0, published)
	if seq == 0 {
		t.Fatal("Append of a published event returned sequence 0")
	}
	if got := appended(t, j, "journal.pub", 0, published); got != seq {
		t.Errorf("appending the published event to its type returned %d, want %d", got, seq)
	}
	appended(t, j, "journal.other", 0, ev("fresh", "t"))

	e := ev("appended", "journal.app")
	first := appended(t, j, "journal.app", 0, e)
	publish(t, j, e)
	appended(t, j, "journal.app", first, ev("next", "t"))
}

func testAppendRejectsInvalid(t *testing.T, j journal) {
	lf, cr := ev("lf", "t"), ev("cr", "t")
	lf.Subject = "a\nb"
	cr.Extensions = map[string]string{"note": "a\rb"}
	for what, e := range map[string]event.Event{
		"an invalid event":       {ID: "no-source", Type: "t"},
		"a type CheckType fails": ev("bad-type", "a..b"),
		"a header holding LF":    lf,
		"a header holding CR":    cr,
	} {
		if _, err := j.Append(context.Background(), "journal.a", 0, e); err == nil {
			t.Errorf("Append accepted %s", what)
		}
	}
	for _, bad := range []string{"", "a..b", ".a", "a.", "a.*", "a.>", "a b"} {
		if _, err := j.Append(context.Background(), bad, 0, ev("bad-subject", "t")); err == nil {
			t.Errorf("Append accepted subject %q", bad)
		}
	}
	appended(t, j, "journal.a", 0, ev("valid", "t"))
}

func read(t *testing.T, j journal, subject string, after uint64) []messaging.Entry {
	t.Helper()
	var entries []messaging.Entry
	for entry, err := range j.Read(context.Background(), subject, after) {
		if err != nil {
			t.Fatalf("Read %s after %d: %v", subject, after, err)
		}
		entries = append(entries, entry)
	}
	return entries
}

func appendedWithin(t *testing.T, j journal, subject string, expect uint64, e event.Event) uint64 {
	t.Helper()
	type result struct {
		seq uint64
		err error
	}
	done := make(chan result, 1)
	go func() {
		seq, err := j.Append(context.Background(), subject, expect, e)
		done <- result{seq, err}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("Append %s to %s at %d: %v", e.ID, subject, expect, r.err)
		}
		return r.seq
	case <-time.After(failsafe):
		t.Fatalf("Append %s to %s did not return: a read still holds the journal", e.ID, subject)
		return 0
	}
}

func ids(entries []messaging.Entry) []string {
	var got []string
	for _, entry := range entries {
		got = append(got, entry.Event.ID)
	}
	return got
}

func testReadInOrder(t *testing.T, j journal) {
	full := event.Event{
		ID:              "full",
		Source:          "/grants",
		Type:            "lab.grant.approved",
		Subject:         "grant/42",
		DataContentType: "application/json",
		DataSchema:      "https://example.com/schemas/grant.json",
		Time:            time.Date(2026, 9, 24, 13, 0, 0, 123456789, time.UTC),
		Data:            []byte(`{"id":42}`),
		Extensions:      map[string]string{"traceparent": "00-abc-def-01"},
	}
	first := appended(t, j, "journal.a", 0, full)
	appended(t, j, "journal.b", 0, ev("other", "t"))
	publish(t, j, ev("published", "journal.pub"))
	second := appended(t, j, "journal.a", first, ev("2", "t"))
	third := appended(t, j, "journal.a", second, ev("3", "t"))

	all := read(t, j, "journal.a", 0)
	if got, want := ids(all), []string{"full", "2", "3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("read %v, want %v: only the subject's events, in sequence order", got, want)
	}
	for i, seq := range []uint64{first, second, third} {
		if all[i].Sequence != seq {
			t.Errorf("entry %s has sequence %d, want its append's %d", all[i].Event.ID, all[i].Sequence, seq)
		}
	}
	if !reflect.DeepEqual(all[0].Event, full) {
		t.Errorf("read back:\n got %+v\nwant %+v", all[0].Event, full)
	}
	if got, want := ids(read(t, j, "journal.a", first)), []string{"2", "3"}; !reflect.DeepEqual(got, want) {
		t.Errorf("read after %d: %v, want %v", first, got, want)
	}
	if got := read(t, j, "journal.a", third); len(got) != 0 {
		t.Errorf("read after the last event yielded %v, want nothing", ids(got))
	}
	if got := read(t, j, "journal.empty", 0); len(got) != 0 {
		t.Errorf("read of a subject with no events yielded %v, want nothing", ids(got))
	}
}

func testReadEndsAtStart(t *testing.T, j journal) {
	first := appended(t, j, "journal.a", 0, ev("1", "t"))
	last := appended(t, j, "journal.a", first, ev("2", "t"))
	var got []string
	for entry, err := range j.Read(context.Background(), "journal.a", 0) {
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		got = append(got, entry.Event.ID)
		if entry.Sequence == first {
			last = appendedWithin(t, j, "journal.a", last, ev("during", "t"))
		}
	}
	if want := []string{"1", "2"}; !reflect.DeepEqual(got, want) {
		t.Errorf("read %v, want %v: an event appended during the read is not yielded", got, want)
	}
	if got, want := ids(read(t, j, "journal.a", 0)), []string{"1", "2", "during"}; !reflect.DeepEqual(got, want) {
		t.Errorf("the next read: %v, want %v", got, want)
	}
}

func testReadReleasesOnBreak(t *testing.T, j journal) {
	first := appended(t, j, "journal.a", 0, ev("1", "t"))
	last := appended(t, j, "journal.a", first, ev("2", "t"))
	for _, err := range j.Read(context.Background(), "journal.a", 0) {
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		break
	}
	appendedWithin(t, j, "journal.a", last, ev("after", "t"))
	if got, want := ids(read(t, j, "journal.a", 0)), []string{"1", "2", "after"}; !reflect.DeepEqual(got, want) {
		t.Errorf("the read after the break: %v, want %v", got, want)
	}
}

func testAppendReachesSubscriptions(t *testing.T, j journal) {
	var d deliveries
	member(t, j, messaging.Subscription{Name: "all"}, d.record("m"))
	a := appended(t, j, "journal.a", 0, ev("1", "t"))
	appended(t, j, "journal.b", 0, ev("2", "t"))
	publish(t, j, ev("3", "t"))
	appended(t, j, "journal.a", a, ev("end", "t"))
	eventually(t, d.has("end"), "the last appended event")
	if got, want := d.seen(), []string{"1", "2", "3", "end"}; !reflect.DeepEqual(got, want) {
		t.Errorf("delivered %v, want %v", got, want)
	}
}

func testAppendFilterMatchesSubject(t *testing.T, j journal) {
	var d deliveries
	member(t, j, messaging.Subscription{Name: "subject", Types: []string{"journal.a"}}, d.record("m"))
	a := appended(t, j, "journal.a", 0, ev("on-subject", "journal.b"))
	appended(t, j, "journal.b", 0, ev("on-type", "journal.a"))
	appended(t, j, "journal.a", a, ev("end", "t"))
	eventually(t, d.has("end"), "the last event on the filtered subject")
	if got, want := d.seen(), []string{"on-subject", "end"}; !reflect.DeepEqual(got, want) {
		t.Errorf("delivered %v, want %v: the filter matches the subject, not the type", got, want)
	}
}
