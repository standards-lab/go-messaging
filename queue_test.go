package messaging_test

import (
	"context"
	"maps"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-messaging"
)

func TestWorkerValidate(t *testing.T) {
	valid := messaging.Worker{Name: "jobs", MaxDeliver: 3, Backoff: []time.Duration{0, time.Second}, DeadLetter: "jobs.dead"}
	if err := valid.Validate(); err != nil {
		t.Errorf("valid worker: %v", err)
	}
	drainer := messaging.Worker{Name: "dead", Subject: "jobs.dead", MaxDeliver: 1, DeadLetter: "jobs.dead.again"}
	if err := drainer.Validate(); err != nil {
		t.Errorf("valid dead-letter worker: %v", err)
	}
	cases := map[string]struct {
		w    messaging.Worker
		want string
	}{
		"no name":          {messaging.Worker{MaxDeliver: 1, DeadLetter: "d"}, "name is required"},
		"dotted name":      {messaging.Worker{Name: "a.b", MaxDeliver: 1, DeadLetter: "d"}, "must not contain"},
		"wildcard subject": {messaging.Worker{Name: "a", Subject: "a.*", MaxDeliver: 1, DeadLetter: "d"}, "subject"},
		"zero max":         {messaging.Worker{Name: "a", DeadLetter: "d"}, "max deliver must be positive"},
		"negative max":     {messaging.Worker{Name: "a", MaxDeliver: -1, DeadLetter: "d"}, "max deliver must be positive"},
		"negative wait":    {messaging.Worker{Name: "a", MaxDeliver: 1, AckWait: -1, DeadLetter: "d"}, "ack wait"},
		"negative backoff": {messaging.Worker{Name: "a", MaxDeliver: 1, Backoff: []time.Duration{time.Second, -1}, DeadLetter: "d"}, "backoff[1]"},
		"no dead letter":   {messaging.Worker{Name: "a", MaxDeliver: 1}, "dead letter: a subject is required"},
		"bad dead letter":  {messaging.Worker{Name: "a", MaxDeliver: 1, DeadLetter: "d.>"}, "dead letter"},
		"own dead letter":  {messaging.Worker{Name: "a", Subject: "s", MaxDeliver: 1, DeadLetter: "s"}, "own subject"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			err := c.w.Validate()
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %v, want it to mention %q", err, c.want)
			}
		})
	}
	t.Run("every problem", func(t *testing.T) {
		w := messaging.Worker{Name: "a b", Subject: "s.*", AckWait: -1, Backoff: []time.Duration{-1, -2}}
		err := w.Validate()
		if err == nil {
			t.Fatal("Validate accepted an unusable worker")
		}
		for _, want := range []string{"name", "subject", "max deliver", "ack wait", "backoff[0]", "backoff[1]", "dead letter"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("err = %v, want it to mention %q", err, want)
			}
		}
	})
}

func TestWorkerNormalize(t *testing.T) {
	backoff := []time.Duration{time.Second}
	w := messaging.Worker{Name: "a", Backoff: backoff}.Normalize()
	if w.AckWait != messaging.DefaultAckWait {
		t.Errorf("AckWait = %v, want %v", w.AckWait, messaging.DefaultAckWait)
	}
	backoff[0] = time.Hour
	if w.Backoff[0] != time.Second {
		t.Error("Normalize must copy Backoff, so the caller cannot change it afterward")
	}
	if got := (messaging.Worker{Backoff: []time.Duration{}}).Normalize().Backoff; got != nil {
		t.Errorf("empty Backoff normalized to %#v, want nil", got)
	}
	if got := (messaging.Worker{AckWait: time.Minute}).Normalize().AckWait; got != time.Minute {
		t.Errorf("a set AckWait normalized to %v, want it kept", got)
	}
}

func TestWorkerRetryDelay(t *testing.T) {
	w := messaging.Worker{Backoff: []time.Duration{time.Second, 5 * time.Second, time.Minute}}
	for attempt, want := range map[int]time.Duration{
		0: time.Second,
		1: time.Second,
		2: 5 * time.Second,
		3: time.Minute,
		4: time.Minute,
		9: time.Minute,
	} {
		if got := w.RetryDelay(attempt); got != want {
			t.Errorf("RetryDelay(%d) = %v, want %v", attempt, got, want)
		}
	}
	if got := (messaging.Worker{}).RetryDelay(2); got != 0 {
		t.Errorf("RetryDelay with no Backoff = %v, want 0", got)
	}
}

func TestWorkerFinal(t *testing.T) {
	w := messaging.Worker{MaxDeliver: 3}
	ctx := context.Background()
	if w.Final(ctx) {
		t.Error("a context with no attempt must not be final")
	}
	for attempt, want := range map[int]bool{1: false, 2: false, 3: true} {
		if got := w.Final(messaging.WithAttempt(ctx, attempt)); got != want {
			t.Errorf("Final on attempt %d = %v, want %v", attempt, got, want)
		}
	}
}

func TestDeadLetterRoundTrip(t *testing.T) {
	for name, d := range map[string]messaging.DeadLetter{
		"errored":  {Attempts: 3, Errored: true, Err: "upstream refused"},
		"no error": {Attempts: 5},
		"empty":    {Attempts: 2, Errored: true},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := messaging.ReadDeadLetter(d.Header())
			if err != nil {
				t.Fatalf("ReadDeadLetter: %v", err)
			}
			if !reflect.DeepEqual(got, d) {
				t.Errorf("ReadDeadLetter = %+v, want %+v", got, d)
			}
		})
	}
	t.Run("no error header says so", func(t *testing.T) {
		h := messaging.DeadLetter{Attempts: 5}.Header()
		if got := h[messaging.HeaderOutcome]; len(got) != 1 || got[0] != messaging.OutcomeNoError {
			t.Errorf("%s = %v, want %q", messaging.HeaderOutcome, got, messaging.OutcomeNoError)
		}
		if _, ok := h[messaging.HeaderLastError]; ok {
			t.Errorf("%s is set on a dead letter with no error", messaging.HeaderLastError)
		}
	})
	t.Run("line breaks", func(t *testing.T) {
		h := messaging.DeadLetter{Attempts: 1, Errored: true, Err: "a\r\nb"}.Header()
		if got := h[messaging.HeaderLastError][0]; strings.ContainsAny(got, "\r\n") {
			t.Errorf("%s = %q, want no CR or LF", messaging.HeaderLastError, got)
		}
	})
	t.Run("canonicalized names", func(t *testing.T) {
		h := event.Header{"Messaging-Attempts": {"4"}, "Messaging-Outcome": {"error"}, "Messaging-Last-Error": {"boom"}}
		got, err := messaging.ReadDeadLetter(h)
		if err != nil {
			t.Fatalf("ReadDeadLetter: %v", err)
		}
		if want := (messaging.DeadLetter{Attempts: 4, Errored: true, Err: "boom"}); got != want {
			t.Errorf("ReadDeadLetter = %+v, want %+v", got, want)
		}
	})
	t.Run("event unchanged", func(t *testing.T) {
		e := event.Event{ID: "1", Source: "/t", Type: "a.b", Data: []byte(`{}`)}
		h, body, err := messaging.Encode(e)
		if err != nil {
			t.Fatal(err)
		}
		maps.Copy(h, (messaging.DeadLetter{Attempts: 2, Errored: true, Err: "x"}).Header())
		got, err := event.Decode(h, body)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if !reflect.DeepEqual(got, e) {
			t.Errorf("decoded dead letter = %+v, want the event %+v", got, e)
		}
	})
	for name, h := range map[string]event.Header{
		"no attempts":    {messaging.HeaderOutcome: {messaging.OutcomeNoError}},
		"bad attempts":   {messaging.HeaderAttempts: {"x"}, messaging.HeaderOutcome: {messaging.OutcomeNoError}},
		"no outcome":     {messaging.HeaderAttempts: {"1"}},
		"bad outcome":    {messaging.HeaderAttempts: {"1"}, messaging.HeaderOutcome: {"maybe"}},
		"negative count": {messaging.HeaderAttempts: {"-1"}, messaging.HeaderOutcome: {messaging.OutcomeNoError}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := messaging.ReadDeadLetter(h); err == nil {
				t.Error("ReadDeadLetter accepted malformed headers")
			}
		})
	}
}

func TestDeadLetterOf(t *testing.T) {
	if _, ok := messaging.DeadLetterOf(context.Background()); ok {
		t.Error("a bare context must carry no dead letter")
	}
	d := messaging.DeadLetter{Attempts: 3, Errored: true, Err: "boom"}
	got, ok := messaging.DeadLetterOf(messaging.WithDeadLetter(context.Background(), d))
	if !ok || got != d {
		t.Errorf("DeadLetterOf = %+v, %v; want %+v, true", got, ok, d)
	}
}
