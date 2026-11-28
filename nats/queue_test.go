package nats_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/standards-lab/go-core/config"
	"github.com/standards-lab/go-core/event"

	"github.com/standards-lab/go-messaging"
	"github.com/standards-lab/go-messaging/nats"
)

func TestQueueConfigValidateRejects(t *testing.T) {
	owner := new(true)
	for name, tc := range map[string]struct {
		cfg  nats.QueueConfig
		want string
	}{
		"no url":                  {nats.QueueConfig{Stream: "s", Subject: "j", DeadLetter: "d"}, "url"},
		"no stream":               {nats.QueueConfig{URL: "u", Subject: "j", DeadLetter: "d"}, "stream"},
		"dotted stream":           {nats.QueueConfig{URL: "u", Stream: "a.b", Subject: "j", DeadLetter: "d"}, "stream"},
		"no subject":              {nats.QueueConfig{URL: "u", Stream: "s", DeadLetter: "d"}, "subject"},
		"wildcard subject":        {nats.QueueConfig{URL: "u", Stream: "s", Subject: "j.*", DeadLetter: "d"}, "subject"},
		"no dead letter":          {nats.QueueConfig{URL: "u", Stream: "s", Subject: "j"}, "dead_letter"},
		"wildcard dead letter":    {nats.QueueConfig{URL: "u", Stream: "s", Subject: "j", DeadLetter: "d.>"}, "dead_letter"},
		"dead letter is subject":  {nats.QueueConfig{URL: "u", Stream: "s", Subject: "j", DeadLetter: "j"}, `subject "j" must not be the dead-letter subject "j" or under it`},
		"subject under dead":      {nats.QueueConfig{URL: "u", Stream: "s", Subject: "d.j", DeadLetter: "d"}, `subject "d.j" must not be the dead-letter subject "d" or under it`},
		"negative max_age":        {nats.QueueConfig{URL: "u", Stream: "s", Subject: "j", DeadLetter: "d", Owner: owner, MaxAge: -1}, "max_age must not be negative"},
		"max_age below the dedup": {nats.QueueConfig{URL: "u", Stream: "s", Subject: "j", DeadLetter: "d", Owner: owner, MaxAge: config.Duration(time.Minute)}, "max_age 1m0s must be at least the deduplication window, 2m0s"},
		"non-owner max_age":       {nats.QueueConfig{URL: "u", Stream: "s", Subject: "j", DeadLetter: "d", MaxAge: config.Duration(time.Hour)}, "max_age is set only on the stream's owner"},
	} {
		t.Run(name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Validate = %v, want an error naming %q", err, tc.want)
			}
			if _, err := nats.NewQueue(tc.cfg); err == nil {
				t.Errorf("NewQueue accepted %+v", tc.cfg)
			}
		})
	}
}

func TestQueueConfigValidateAccepts(t *testing.T) {
	for name, cfg := range map[string]nats.QueueConfig{
		"non-owner":            {URL: "u", Stream: "s", Subject: "jobs", DeadLetter: "jobs.dead"},
		"owner, max_age":       {URL: "u", Stream: "s", Subject: "a.jobs", DeadLetter: "a.dead", Owner: new(true), MaxAge: config.Duration(time.Hour)},
		"owner false, nothing": {URL: "u", Stream: "s", Subject: "jobs", DeadLetter: "dead", Owner: new(false)},
	} {
		if err := cfg.Validate(); err != nil {
			t.Errorf("%s: Validate = %v", name, err)
		}
	}
}

func TestQueueConfigMerge(t *testing.T) {
	cfg := nats.QueueConfig{URL: "base", Name: "base", Stream: "base", Subject: "base", DeadLetter: "base", Owner: new(true), MaxAge: 1}
	cfg.Merge(nil)
	cfg.Merge(&nats.QueueConfig{})
	if cfg.URL != "base" || cfg.DeadLetter != "base" || cfg.Owner == nil || !*cfg.Owner || cfg.MaxAge != 1 {
		t.Fatalf("an empty overlay changed the config: %+v", cfg)
	}

	var overlay nats.QueueConfig
	if err := json.Unmarshal([]byte(`{"url":"u","name":"n","stream":"s","subject":"j","dead_letter":"d","owner":false,"max_age":"1h"}`), &overlay); err != nil {
		t.Fatal(err)
	}
	cfg.Merge(&overlay)
	want := nats.QueueConfig{URL: "u", Name: "n", Stream: "s", Subject: "j", DeadLetter: "d", MaxAge: config.Duration(time.Hour)}
	if cfg.Owner == nil || *cfg.Owner {
		t.Errorf("Owner = %v, want an Owner set to false", cfg.Owner)
	}
	cfg.Owner = nil
	if cfg != want {
		t.Errorf("Merge = %+v, want %+v", cfg, want)
	}
}

func TestQueueConfigFinalize(t *testing.T) {
	cfg := nats.QueueConfig{Stream: "s", Subject: "j", DeadLetter: "d"}
	if err := cfg.Finalize(""); err != nil {
		t.Fatal(err)
	}
	if cfg.URL != nats.DefaultURL {
		t.Errorf("URL = %q, want %q", cfg.URL, nats.DefaultURL)
	}

	cfg = nats.QueueConfig{Stream: "file", Subject: "file", DeadLetter: "file.dead"}
	t.Setenv("SVC_NATS_QUEUE_URL", "nats://env:4222")
	t.Setenv("SVC_NATS_QUEUE_NAME", "env")
	t.Setenv("SVC_NATS_QUEUE_STREAM", "env")
	t.Setenv("SVC_NATS_QUEUE_SUBJECT", "env.jobs")
	t.Setenv("SVC_NATS_QUEUE_DEAD_LETTER", "env.dead")
	t.Setenv("SVC_NATS_QUEUE_OWNER", "true")
	t.Setenv("SVC_NATS_QUEUE_MAX_AGE", "2h")
	if err := cfg.Finalize("svc"); err != nil {
		t.Fatal(err)
	}
	if cfg.Owner == nil || !*cfg.Owner {
		t.Errorf("Owner = %v, want true", cfg.Owner)
	}
	cfg.Owner = nil
	want := nats.QueueConfig{URL: "nats://env:4222", Name: "env", Stream: "env", Subject: "env.jobs", DeadLetter: "env.dead", MaxAge: config.Duration(2 * time.Hour)}
	if cfg != want {
		t.Errorf("Finalize = %+v, want %+v", cfg, want)
	}

	for key, value := range map[string]string{"SVC_NATS_QUEUE_OWNER": "maybe", "SVC_NATS_QUEUE_MAX_AGE": "later"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, value)
			cfg := nats.QueueConfig{Stream: "s", Subject: "j", DeadLetter: "d"}
			if err := cfg.Finalize("svc"); err == nil || !strings.Contains(err.Error(), key) {
				t.Errorf("Finalize = %v, want an error naming %s", err, key)
			}
		})
	}
}

func newQueue(t *testing.T, url string) *nats.Queue {
	t.Helper()
	q, err := nats.NewQueue(nats.QueueConfig{URL: url, Stream: "test", Subject: "test.jobs", DeadLetter: "test.dead"})
	if err != nil {
		t.Fatal(err)
	}
	return q
}

var queueWorker = messaging.Worker{Name: "jobs", MaxDeliver: 3, DeadLetter: "test.dead"}

func TestQueueBeforeStart(t *testing.T) {
	q := newQueue(t, closedURL(t))
	if q.Ready() {
		t.Error("an unstarted queue is ready")
	}
	if err := q.Enqueue(t.Context(), event.Event{ID: "1", Source: "/test", Type: "t"}); !errors.Is(err, nats.ErrNotStarted) {
		t.Errorf("Enqueue = %v, want ErrNotStarted", err)
	}
	if err := q.Shutdown(t.Context()); err != nil {
		t.Errorf("Shutdown of an unstarted queue = %v", err)
	}
	src, err := q.Subscribe(queueWorker, nil)
	if err != nil {
		t.Fatal(err)
	}
	if src.Ready() {
		t.Error("a source is ready before it receives")
	}
	err = src.Receive(t.Context(), func(context.Context, event.Event) error { return nil })
	if !errors.Is(err, nats.ErrNotStarted) {
		t.Errorf("Receive = %v, want ErrNotStarted", err)
	}
}

func TestQueueStartFailsWithoutNATS(t *testing.T) {
	q := newQueue(t, closedURL(t))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := q.Start(ctx); err == nil {
		t.Fatal("Start succeeded against a closed port")
	}
	if q.Ready() {
		t.Error("a queue that failed to start is ready")
	}
	if err := q.Start(ctx); err == nil || errors.Is(err, nats.ErrStarted) {
		t.Errorf("a Start after a failed one = %v, want the connection's failure", err)
	}
}

func TestEnqueueRejects(t *testing.T) {
	q := newQueue(t, closedURL(t))
	for name, e := range map[string]event.Event{
		"no id":         {Source: "/test", Type: "t"},
		"wildcard type": {ID: "1", Source: "/test", Type: "t.*"},
		"CR LF":         {ID: "1", Source: "/test", Type: "t", Extensions: map[string]string{"note": "two\r\nlines"}},
		"CR in subject": {ID: "1", Source: "/test", Type: "t", Subject: "a\rb"},
	} {
		err := q.Enqueue(t.Context(), e)
		if err == nil || errors.Is(err, nats.ErrNotStarted) {
			t.Errorf("%s: Enqueue = %v, want a rejection", name, err)
		}
	}
}

func TestQueueSubscribeChecksSubjects(t *testing.T) {
	q := newQueue(t, closedURL(t))
	for name, tc := range map[string]struct {
		w    messaging.Worker
		want string
	}{
		"invalid worker":          {messaging.Worker{Name: "a.b", MaxDeliver: 1, DeadLetter: "test.dead"}, "name"},
		"dead letter elsewhere":   {messaging.Worker{Name: "w", MaxDeliver: 1, DeadLetter: "other.dead"}, `dead letter "other.dead"`},
		"dead letter is the work": {messaging.Worker{Name: "w", MaxDeliver: 1, DeadLetter: "test.jobs"}, `dead letter "test.jobs"`},
		"subject elsewhere":       {messaging.Worker{Name: "w", Subject: "test.jobs", MaxDeliver: 1, DeadLetter: "test.dead"}, `subject "test.jobs"`},
	} {
		if _, err := q.Subscribe(tc.w, nil); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: Subscribe = %v, want an error naming %s", name, err, tc.want)
		}
	}
	for name, w := range map[string]messaging.Worker{
		"work":             queueWorker,
		"dead letters":     {Name: "w", Subject: "test.dead", MaxDeliver: 1, DeadLetter: "test.dead.dead"},
		"under dead":       {Name: "w", MaxDeliver: 1, DeadLetter: "test.dead.jobs"},
		"under dead again": {Name: "w", Subject: "test.dead.jobs", MaxDeliver: 1, DeadLetter: "test.dead"},
	} {
		if _, err := q.Subscribe(w, nil); err != nil {
			t.Errorf("%s: Subscribe = %v", name, err)
		}
	}
}

func TestQueueSubscribeBoundsAckWait(t *testing.T) {
	q := newQueue(t, closedURL(t))
	limit := messaging.DefaultDuplicates - nats.AckMargin
	for name, ackWait := range map[string]time.Duration{
		"at the window":    limit,
		"the whole window": messaging.DefaultDuplicates,
		"an hour":          time.Hour,
	} {
		w := queueWorker
		w.AckWait = ackWait
		_, err := q.Subscribe(w, nil)
		if err == nil || !strings.Contains(err.Error(), "set AckWait below "+limit.String()) {
			t.Errorf("%s: Subscribe = %v, want an error naming AckWait's bound", name, err)
		}
	}
	w := queueWorker
	w.AckWait = limit - time.Millisecond
	if _, err := q.Subscribe(w, nil); err != nil {
		t.Errorf("Subscribe with AckWait %v = %v", w.AckWait, err)
	}

	w = messaging.Worker{Name: "w", MaxDeliver: 1, AckWait: time.Hour, DeadLetter: "other.dead"}
	_, err := q.Subscribe(w, nil)
	for _, want := range []string{`dead letter "other.dead"`, "set AckWait below"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Subscribe = %v, want an error naming %s", err, want)
		}
	}
}
