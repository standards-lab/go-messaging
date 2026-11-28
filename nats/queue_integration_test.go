//go:build integration

package nats_test

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	natsgo "github.com/nats-io/nats.go"
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
			Queue:      q,
			Unit:       250 * time.Millisecond,
			Advance:    time.Sleep,
			DeadLetter: dead,
			Runtime:    workRuntime(t),
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
		if got.MaxDeliver != -1 {
			t.Errorf("%s: MaxDeliver = %d, want -1, unlimited", w.Name, got.MaxDeliver)
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

func held(t *testing.T, js jetstream.JetStream, stream, subject string) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	s, err := js.Stream(ctx, stream)
	if err != nil {
		t.Fatalf("stream %s: %v", stream, err)
	}
	info, err := s.Info(ctx, jetstream.WithSubjectFilter(subject))
	if err != nil {
		t.Fatalf("stream %s info: %v", stream, err)
	}
	return info.State.Subjects[subject]
}

func holding(t *testing.T, js jetstream.JetStream, stream string, want map[string]uint64) func() bool {
	return func() bool {
		for subject, n := range want {
			if held(t, js, stream, subject) != n {
				return false
			}
		}
		return true
	}
}

func rawPublish(t *testing.T, js jetstream.JetStream, msg *natsgo.Msg) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	if _, err := js.PublishMsg(ctx, msg); err != nil {
		t.Fatalf("publish on %s: %v", msg.Subject, err)
	}
}

func TestQueueDeadLetterStoredOnce(t *testing.T) {
	q, url, cfg := ownerQueue(t)
	js := jetStream(t, url)
	w := messaging.Worker{Name: "once", MaxDeliver: 3, DeadLetter: cfg.DeadLetter}
	src, err := q.Subscribe(w, nil)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	receive(t, src, func(context.Context, event.Event) error {
		calls.Add(1)
		return event.Permanent(errors.New("malformed"))
	})
	e := event.Event{ID: "x", Source: "/test", Type: "t"}
	if err := q.Enqueue(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the dead letter, and the job gone", holding(t, js, cfg.Stream, map[string]uint64{cfg.DeadLetter: 1, cfg.Subject: 0}))

	h, body, err := messaging.Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	again := &natsgo.Msg{Subject: cfg.Subject, Header: natsgo.Header(h), Data: body}
	again.Header.Set(jetstream.MsgIDHeader, "again")
	rawPublish(t, js, again)
	eventually(t, "the repeat handled and gone", func() bool {
		return calls.Load() == 2 && held(t, js, cfg.Stream, cfg.Subject) == 0
	})
	if n := held(t, js, cfg.Stream, cfg.DeadLetter); n != 1 {
		t.Errorf("the dead-letter subject holds %d messages after the event was dead-lettered twice, want 1", n)
	}
}

func TestQueueDeadLetterStoredBeforeRemoval(t *testing.T) {
	q, url, cfg := ownerQueue(t)
	js := jetStream(t, url)
	full := streamInfo(t, js, cfg.Stream).Config
	cut := full
	cut.Subjects = []string{cfg.Subject}
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	if _, err := js.UpdateStream(ctx, cut); err != nil {
		t.Fatal(err)
	}
	w := messaging.Worker{Name: "before", MaxDeliver: 20, Backoff: []time.Duration{200 * time.Millisecond}, DeadLetter: cfg.DeadLetter}
	src, err := q.Subscribe(w, nil)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	receive(t, src, func(context.Context, event.Event) error {
		calls.Add(1)
		return event.Permanent(errors.New("malformed"))
	})
	if err := q.Enqueue(t.Context(), event.Event{ID: "x", Source: "/test", Type: "t"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the job's redelivery after its dead letter failed", func() bool { return calls.Load() >= 2 })
	if n := held(t, js, cfg.Stream, cfg.Subject); n != 1 {
		t.Errorf("the work subject holds %d jobs while the dead letter cannot be stored, want 1", n)
	}

	if _, err := js.UpdateStream(ctx, full); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the dead letter, and the job gone", holding(t, js, cfg.Stream, map[string]uint64{cfg.DeadLetter: 1, cfg.Subject: 0}))
}

func TestQueueDeadLetterPastLimitRetried(t *testing.T) {
	q, url, cfg := ownerQueue(t)
	js := jetStream(t, url)
	full := streamInfo(t, js, cfg.Stream).Config
	cut := full
	cut.Subjects = []string{cfg.Subject}
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	if _, err := js.UpdateStream(ctx, cut); err != nil {
		t.Fatal(err)
	}
	w := messaging.Worker{Name: "past", MaxDeliver: 2, Backoff: []time.Duration{200 * time.Millisecond}, DeadLetter: cfg.DeadLetter}
	var failed reports
	src, err := q.Subscribe(w, failed.report)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	receive(t, src, func(context.Context, event.Event) error {
		calls.Add(1)
		return errors.New("transient")
	})
	if err := q.Enqueue(t.Context(), event.Event{ID: "x", Source: "/test", Type: "t"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "a delivery past the limit after one past it failed to dead-letter", func() bool {
		n := 0
		for _, err := range failed.seen() {
			if strings.Contains(err.Error(), "dead-letter") {
				n++
			}
		}
		return n >= 3
	})
	if n := held(t, js, cfg.Stream, cfg.Subject); n != 1 {
		t.Errorf("the work subject holds %d jobs while the dead letter cannot be stored, want 1", n)
	}

	if _, err := js.UpdateStream(ctx, full); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the dead letter, and the job gone", holding(t, js, cfg.Stream, map[string]uint64{cfg.DeadLetter: 1, cfg.Subject: 0}))
	if n := calls.Load(); n != int32(w.MaxDeliver) {
		t.Errorf("the handler ran %d times, want MaxDeliver, %d", n, w.MaxDeliver)
	}
}

func TestQueueUndecodableDeadLetters(t *testing.T) {
	q, url, cfg := ownerQueue(t)
	js := jetStream(t, url)
	w := messaging.Worker{Name: "undecodable", MaxDeliver: 3, DeadLetter: cfg.DeadLetter}
	src, err := q.Subscribe(w, nil)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	receive(t, src, func(context.Context, event.Event) error {
		calls.Add(1)
		return nil
	})
	rawPublish(t, js, &natsgo.Msg{Subject: cfg.Subject, Header: natsgo.Header{"note": {"kept"}}, Data: []byte("not an event")})
	eventually(t, "the dead letter, and the message gone", holding(t, js, cfg.Stream, map[string]uint64{cfg.DeadLetter: 1, cfg.Subject: 0}))
	if n := calls.Load(); n != 0 {
		t.Errorf("the handler ran %d times on an undecodable message, want none", n)
	}
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	s, err := js.Stream(ctx, cfg.Stream)
	if err != nil {
		t.Fatal(err)
	}
	m, err := s.GetLastMsgForSubject(ctx, cfg.DeadLetter)
	if err != nil {
		t.Fatal(err)
	}
	if string(m.Data) != "not an event" || m.Header.Get("note") != "kept" {
		t.Errorf("dead letter %q with note %q, want the message unchanged", m.Data, m.Header.Get("note"))
	}
	d, err := messaging.ReadDeadLetter(event.Header(m.Header))
	if err != nil {
		t.Fatal(err)
	}
	if d.Attempts != 1 || !d.Errored || d.Err == "" {
		t.Errorf("dead letter %+v, want one attempt ending in the decoding error", d)
	}
}
