package messagingtest

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

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
		{"AppendDeduplicates", testAppendDeduplicates},
		{"AppendSharesPublishWindow", testAppendSharesPublishWindow},
		{"AppendRejectsInvalid", testAppendRejectsInvalid},
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
