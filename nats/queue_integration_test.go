//go:build integration

package nats_test

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"slices"
	"testing"
	"testing/fstest"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/standards-lab/go-core/config"
	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/sqlate/query"
	"github.com/standards-lab/sqlate/sqltest"

	"github.com/standards-lab/go-messaging"
	"github.com/standards-lab/go-messaging/inbox"
	"github.com/standards-lab/go-messaging/memory"
	"github.com/standards-lab/go-messaging/messagingtest"
	"github.com/standards-lab/go-messaging/nats"
	"github.com/standards-lab/go-messaging/nats/natstest"
	"github.com/standards-lab/go-messaging/outbox"
)

func TestQueueConformance(t *testing.T) {
	url := natstest.Open(t)
	messagingtest.RunQueue(t, func(t *testing.T) messagingtest.QueueHarness {
		stream, subject, dead := natstest.ScratchQueue(t, url)
		q := startedQueue(t, nats.QueueConfig{URL: url, Stream: stream, Subject: subject, DeadLetter: dead, Owner: new(true)})
		return messagingtest.QueueHarness{
			Queue:          q,
			Unit:           250 * time.Millisecond,
			Advance:        time.Sleep,
			DeadLetter:     dead,
			Runtime:        workRuntime(t),
			SkipDeadLetter: true,
		}
	})
}

func startedQueue(t *testing.T, cfg nats.QueueConfig) *nats.Queue {
	t.Helper()
	cfg.Name = t.Name()
	q, err := nats.NewQueue(cfg)
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	if err := q.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), failsafe)
		defer cancel()
		if err := q.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})
	return q
}

func workRuntime(t *testing.T) *messaging.Runtime {
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
	cfg := messaging.Config{Source: "/test"}
	if err := cfg.Finalize(""); err != nil {
		t.Fatal(err)
	}
	rt, err := messaging.Setup{
		Config: cfg,
		Broker: memory.New(),
		Outbox: outbox.Engine{
			Emit:          s.Statement("emit"),
			ClaimRow:      s.Statement("claim_row"),
			MarkPublished: s.Statement("mark_published"),
			Quarantine:    s.Statement("quarantine"),
		},
		Inbox:    inbox.Engine{Claim: s.Statement("claim")},
		Shutdown: 4 * time.Second,
		Logger:   slog.New(slog.DiscardHandler),
	}.Build()
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

func ownerQueue(t *testing.T) (q *nats.Queue, url string, cfg nats.QueueConfig) {
	t.Helper()
	url = natstest.Open(t)
	stream, subject, dead := natstest.ScratchQueue(t, url)
	cfg = nats.QueueConfig{URL: url, Stream: stream, Subject: subject, DeadLetter: dead, Owner: new(true)}
	return startedQueue(t, cfg), url, cfg
}

func TestQueueOwnerProvisions(t *testing.T) {
	url := natstest.Open(t)
	stream, subject, dead := natstest.ScratchQueue(t, url)
	startedQueue(t, nats.QueueConfig{URL: url, Stream: stream, Subject: subject, DeadLetter: dead, Owner: new(true), MaxAge: config.Duration(time.Hour)})
	got := streamInfo(t, jetStream(t, url), stream).Config
	if got.Retention != jetstream.WorkQueuePolicy || got.Storage != jetstream.FileStorage {
		t.Errorf("Retention, Storage = %v, %v; want work-queue, file", got.Retention, got.Storage)
	}
	if want := []string{subject, dead, dead + ".>"}; !slices.Equal(got.Subjects, want) {
		t.Errorf("Subjects = %q, want %q", got.Subjects, want)
	}
	if got.Duplicates != messaging.DefaultDuplicates || got.MaxAge != time.Hour {
		t.Errorf("Duplicates, MaxAge = %v, %v; want %v, 1h", got.Duplicates, got.MaxAge, messaging.DefaultDuplicates)
	}
}

func TestQueueNonOwnerBinds(t *testing.T) {
	_, url, cfg := ownerQueue(t)
	cfg.Owner = nil
	q := startedQueue(t, cfg)
	if !q.Ready() {
		t.Error("Ready after Start = false, want true")
	}
	before := streamInfo(t, jetStream(t, url), cfg.Stream).Config
	if err := q.Enqueue(t.Context(), event.Event{ID: "1", Source: "/test", Type: "t"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	info := streamInfo(t, jetStream(t, url), cfg.Stream)
	if info.State.Msgs != 1 {
		t.Errorf("the stream holds %d jobs, want 1", info.State.Msgs)
	}
	if !slices.Equal(info.Config.Subjects, before.Subjects) {
		t.Errorf("a non-owner changed the stream's subjects to %q", info.Config.Subjects)
	}
}

func TestQueueNonOwnerBindsCapturingSubjects(t *testing.T) {
	url := natstest.Open(t)
	stream, subject, dead := natstest.ScratchQueue(t, url)
	js := jetStream(t, url)
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	parent := subject[:len(subject)-len(".jobs")]
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: stream, Subjects: []string{parent + ".>"}, Retention: jetstream.WorkQueuePolicy}); err != nil {
		t.Fatal(err)
	}
	startedQueue(t, nats.QueueConfig{URL: url, Stream: stream, Subject: subject, DeadLetter: dead})
}

func TestQueueNonOwnerStartFails(t *testing.T) {
	url := natstest.Open(t)
	for name, tc := range map[string]struct {
		subjects  func(subject, dead string) []string
		retention jetstream.RetentionPolicy
		want      error
	}{
		"missing stream": {want: jetstream.ErrStreamNotFound},
		"limits retention": {
			subjects:  func(subject, dead string) []string { return []string{subject, dead, dead + ".>"} },
			retention: jetstream.LimitsPolicy,
		},
		"no dead letter": {
			subjects:  func(subject, _ string) []string { return []string{subject} },
			retention: jetstream.WorkQueuePolicy,
		},
		"nothing under the dead letter": {
			subjects:  func(subject, dead string) []string { return []string{subject, dead} },
			retention: jetstream.WorkQueuePolicy,
		},
		"no work subject": {
			subjects:  func(_, dead string) []string { return []string{dead, dead + ".>"} },
			retention: jetstream.WorkQueuePolicy,
		},
	} {
		t.Run(name, func(t *testing.T) {
			stream, subject, dead := natstest.ScratchQueue(t, url)
			if tc.subjects != nil {
				ctx, cancel := context.WithTimeout(t.Context(), failsafe)
				defer cancel()
				if _, err := jetStream(t, url).CreateStream(ctx, jetstream.StreamConfig{Name: stream, Subjects: tc.subjects(subject, dead), Retention: tc.retention}); err != nil {
					t.Fatal(err)
				}
			}
			q, err := nats.NewQueue(nats.QueueConfig{URL: url, Stream: stream, Subject: subject, DeadLetter: dead})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), failsafe)
			defer cancel()
			err = q.Start(ctx)
			if err == nil {
				_ = q.Shutdown(ctx)
				t.Fatal("Start bound a stream it cannot use")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Errorf("Start = %v, want %v", err, tc.want)
			}
			if q.Ready() {
				t.Error("a queue that failed to start is ready")
			}
		})
	}
}

func TestQueueSecondStartFails(t *testing.T) {
	q, _, _ := ownerQueue(t)
	if err := q.Start(t.Context()); !errors.Is(err, nats.ErrStarted) {
		t.Errorf("a second Start = %v, want ErrStarted", err)
	}
}

func TestQueueWorkerBindsConsumer(t *testing.T) {
	q, url, cfg := ownerQueue(t)
	for _, w := range []messaging.Worker{
		{Name: "jobs", MaxDeliver: 3, AckWait: 2 * time.Second, DeadLetter: cfg.DeadLetter},
		{Name: "dead", Subject: cfg.DeadLetter, MaxDeliver: 1, DeadLetter: cfg.DeadLetter + ".dead"},
	} {
		src, err := q.Subscribe(w, nil)
		if err != nil {
			t.Fatal(err)
		}
		receive(t, src, func(context.Context, event.Event) error { return nil })
		eventually(t, "the worker to be receiving", src.Ready)
		got := consumerInfo(t, jetStream(t, url), cfg.Stream, w.Name).Config
		want := w.Normalize()
		subject := cmp.Or(want.Subject, cfg.Subject)
		if got.Durable != w.Name || got.DeliverPolicy != jetstream.DeliverAllPolicy || got.AckPolicy != jetstream.AckExplicitPolicy || got.FilterSubject != subject {
			t.Errorf("%s: Durable, DeliverPolicy, AckPolicy, FilterSubject = %s, %v, %v, %s; want %s, all, explicit, %s", w.Name, got.Durable, got.DeliverPolicy, got.AckPolicy, got.FilterSubject, w.Name, subject)
		}
		if got.AckWait != want.AckWait+nats.AckMargin {
			t.Errorf("%s: AckWait = %v, want %v", w.Name, got.AckWait, want.AckWait+nats.AckMargin)
		}
		if got.MaxDeliver != w.MaxDeliver+1 {
			t.Errorf("%s: MaxDeliver = %d, want %d, one past the worker's", w.Name, got.MaxDeliver, w.MaxDeliver+1)
		}
	}
}

func TestQueueLostExtensionEndsHandler(t *testing.T) {
	q, url, cfg := ownerQueue(t)
	w := messaging.Worker{Name: "lost", MaxDeliver: 3, AckWait: time.Second, DeadLetter: cfg.DeadLetter}
	started := make(chan context.Context, 1)
	r, err := workRuntime(t).Work(q, w, func(ctx context.Context, _ struct{}) error {
		started <- ctx
		<-ctx.Done()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), failsafe)
		defer cancel()
		_ = r.Shutdown(ctx)
	})
	if err := q.Enqueue(t.Context(), event.Event{ID: "x", Source: "/test", Type: "t", Data: []byte("{}")}); err != nil {
		t.Fatal(err)
	}
	var hctx context.Context
	select {
	case hctx = <-started:
	case <-time.After(failsafe):
		t.Fatal("timed out waiting for the handler")
	}
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	if err := jetStream(t, url).DeleteConsumer(ctx, cfg.Stream, w.Name); err != nil {
		t.Fatal(err)
	}
	lost := time.Now()
	select {
	case <-hctx.Done():
	case <-time.After(failsafe):
		t.Fatal("the handler's context under a reactor did not end after its extension was lost")
	}
	if waited := time.Since(lost); waited > w.AckWait+w.AckWait/2 {
		t.Errorf("the handler's context ended %v after the extension was lost, want within AckWait %v and an extension's interval", waited, w.AckWait)
	}
}
