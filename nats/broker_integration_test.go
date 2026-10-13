//go:build integration

package nats_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/standards-lab/go-core/config"
	"github.com/standards-lab/go-core/event"

	"github.com/standards-lab/go-messaging"
	"github.com/standards-lab/go-messaging/nats"
	"github.com/standards-lab/go-messaging/nats/natstest"
)

// failsafe bounds each server call a test makes.
const failsafe = 10 * time.Second

// newConfigured builds a broker on cfg, named for the test.
func newConfigured(t *testing.T, cfg nats.Config) *nats.Broker {
	t.Helper()
	cfg.Name = t.Name()
	b, err := nats.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return b
}

// started builds and starts a broker on cfg, shut down when the test ends.
func started(t *testing.T, cfg nats.Config) *nats.Broker {
	t.Helper()
	b := newConfigured(t, cfg)
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	if err := b.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	shutdown(t, b)
	return b
}

// shutdown registers b's Shutdown as a cleanup.
func shutdown(t *testing.T, b *nats.Broker) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), failsafe)
		defer cancel()
		if err := b.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})
}

// jetStream is a JetStream context on its own connection to url, closed when
// the test ends, so the test inspects the server independently of a broker.
func jetStream(t *testing.T, url string) jetstream.JetStream {
	t.Helper()
	nc, err := natsgo.Connect(url, natsgo.Name(t.Name()))
	if err != nil {
		t.Fatalf("connect %s: %v", url, err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return js
}

// streamInfo is the stream's current configuration and state.
func streamInfo(t *testing.T, js jetstream.JetStream, name string) *jetstream.StreamInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	s, err := js.Stream(ctx, name)
	if err != nil {
		t.Fatalf("stream %s: %v", name, err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		t.Fatalf("stream %s info: %v", name, err)
	}
	return info
}

// createStream creates the stream by hand, as another deployment would.
func createStream(t *testing.T, js jetstream.JetStream, name string, subjects ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: subjects}); err != nil {
		t.Fatalf("create stream %s: %v", name, err)
	}
}

// publish publishes e through b, failing the test on an error.
func publish(t *testing.T, b *nats.Broker, e event.Event) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	if err := b.Publish(ctx, e); err != nil {
		t.Fatalf("Publish %s: %v", e.ID, err)
	}
}

// TestBrokerStarts proves Start connects to the compose server, and that
// Shutdown drains and closes the connection it opened.
func TestBrokerStarts(t *testing.T) {
	url := natstest.Open(t)
	stream, prefix := natstest.Scratch(t, url)
	b := newConfigured(t, nats.Config{URL: url, Stream: stream, Prefix: prefix, Owner: new(true)})
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	if err := b.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !b.Ready() {
		t.Error("Ready after Start = false, want true")
	}
	if err := b.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if !b.Conn().IsClosed() {
		t.Error("connection after Shutdown is not closed")
	}
	if b.Ready() {
		t.Error("Ready after Shutdown = true, want false")
	}
}

// The owner provisions the stream with file storage, limits retention, the
// subjects Prefix.>, its deduplication window, and its MaxAge; a window of 0
// is DefaultDuplicates, and a MaxAge of 0 keeps every event.
func TestOwnerProvisionsRetention(t *testing.T) {
	url := natstest.Open(t)
	js := jetStream(t, url)
	for name, c := range map[string]struct {
		duplicates, maxAge, wantDuplicates time.Duration
	}{
		"defaults": {0, 0, messaging.DefaultDuplicates},
		"set":      {30 * time.Second, time.Hour, 30 * time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			stream, prefix := natstest.Scratch(t, url)
			started(t, nats.Config{
				URL: url, Stream: stream, Prefix: prefix, Owner: new(true),
				Duplicates: config.Duration(c.duplicates), MaxAge: config.Duration(c.maxAge),
			})
			got := streamInfo(t, js, stream).Config
			if got.Storage != jetstream.FileStorage {
				t.Errorf("Storage = %v, want file", got.Storage)
			}
			if got.Retention != jetstream.LimitsPolicy {
				t.Errorf("Retention = %v, want limits", got.Retention)
			}
			if want := []string{prefix + ".>"}; !reflect.DeepEqual(got.Subjects, want) {
				t.Errorf("Subjects = %q, want %q", got.Subjects, want)
			}
			if got.Duplicates != c.wantDuplicates {
				t.Errorf("Duplicates = %v, want %v", got.Duplicates, c.wantDuplicates)
			}
			if got.MaxAge != c.maxAge {
				t.Errorf("MaxAge = %v, want %v", got.MaxAge, c.maxAge)
			}
		})
	}
}

// A second owner on the stream, as a redeploy with a new configuration
// starts, converges the stream on its own configuration.
func TestSecondOwnerConverges(t *testing.T) {
	url := natstest.Open(t)
	js := jetStream(t, url)
	stream, prefix := natstest.Scratch(t, url)
	started(t, nats.Config{
		URL: url, Stream: stream, Prefix: prefix, Owner: new(true),
		Duplicates: config.Duration(time.Minute), MaxAge: config.Duration(time.Hour),
	})
	started(t, nats.Config{
		URL: url, Stream: stream, Prefix: prefix, Owner: new(true),
		Duplicates: config.Duration(3 * time.Minute), MaxAge: config.Duration(2 * time.Hour),
	})
	got := streamInfo(t, js, stream).Config
	if got.Duplicates != 3*time.Minute || got.MaxAge != 2*time.Hour {
		t.Errorf("Duplicates, MaxAge = %v, %v; want the second owner's 3m0s, 2h0m0s", got.Duplicates, got.MaxAge)
	}
}

// A non-owner binds the owner's stream without changing its configuration,
// and the events either publishes land in the one stream.
func TestNonOwnerBinds(t *testing.T) {
	url := natstest.Open(t)
	js := jetStream(t, url)
	stream, prefix := natstest.Scratch(t, url)
	own := started(t, nats.Config{
		URL: url, Stream: stream, Prefix: prefix, Owner: new(true),
		Duplicates: config.Duration(time.Minute), MaxAge: config.Duration(time.Hour),
	})
	before := streamInfo(t, js, stream).Config

	other := started(t, nats.Config{URL: url, Stream: stream, Prefix: prefix})
	if after := streamInfo(t, js, stream).Config; !reflect.DeepEqual(after, before) {
		t.Errorf("the non-owner's Start changed the stream:\nbefore %+v\nafter  %+v", before, after)
	}

	publish(t, other, event.Event{ID: "from-non-owner", Source: "/test", Type: "orders.created"})
	publish(t, own, event.Event{ID: "from-owner", Source: "/test", Type: "orders.created"})
	if n := streamInfo(t, js, stream).State.Msgs; n != 2 {
		t.Errorf("stream holds %d messages, want both brokers' 2", n)
	}
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	s, err := js.Stream(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	m, err := s.GetMsg(ctx, 1)
	if err != nil {
		t.Fatalf("get the first message: %v", err)
	}
	if got := m.Header.Get("ce-id"); got != "from-non-owner" {
		t.Errorf("first message's ce-id = %q, want the non-owner's from-non-owner", got)
	}
}

// A non-owner binds a stream one of whose subjects captures every subject
// under its prefix, by NATS's wildcard rules: '*' one token, a final '>' one
// or more. Each stream's subjects sit under the scratch prefix, so no two
// tests' streams overlap; the broker's prefix is two tokens below it.
func TestNonOwnerBindsCapturingSubject(t *testing.T) {
	url := natstest.Open(t)
	js := jetStream(t, url)
	for name, sfx := range map[string][]string{
		"its own .>":        {".a.b.>"},
		"its parent's .>":   {".a.>"},
		"an ancestor's .>":  {".>"},
		"'*' for its last":  {".a.*.>"},
		"'*' for its first": {".*.b.>"},
		"'*' then '>'":      {".*.>"},
		"one of several":    {".x.>", ".a.b.>"},
	} {
		t.Run(name, func(t *testing.T) {
			stream, prefix := natstest.Scratch(t, url)
			var subjects []string
			for _, s := range sfx {
				subjects = append(subjects, prefix+s)
			}
			createStream(t, js, stream, subjects...)
			b := started(t, nats.Config{URL: url, Stream: stream, Prefix: prefix + ".a.b"})
			publish(t, b, event.Event{ID: "1", Source: "/test", Type: "created"})
			if n := streamInfo(t, js, stream).State.Msgs; n != 1 {
				t.Errorf("stream on %q holds %d messages, want 1", subjects, n)
			}
		})
	}
}

// A non-owner's Start fails when its stream is missing, creates nothing,
// leaves the broker unstarted, and succeeds once the owner has provisioned
// the stream.
func TestNonOwnerStartFailsOnMissingStream(t *testing.T) {
	url := natstest.Open(t)
	js := jetStream(t, url)
	stream, prefix := natstest.Scratch(t, url)
	b := newConfigured(t, nats.Config{URL: url, Stream: stream, Prefix: prefix})
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()

	err := b.Start(ctx)
	if !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatalf("Start on a missing stream = %v, want ErrStreamNotFound", err)
	}
	t.Logf("Start on a missing stream: %v", err)
	if b.Ready() || b.Conn() != nil {
		t.Error("a broker whose Start failed is ready, or has a connection")
	}
	if _, err := js.Stream(ctx, stream); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Errorf("stream after the non-owner's Start: err %v, want ErrStreamNotFound", err)
	}

	started(t, nats.Config{URL: url, Stream: stream, Prefix: prefix, Owner: new(true)})
	if err := b.Start(ctx); err != nil {
		t.Fatalf("Start once the stream exists: %v", err)
	}
	shutdown(t, b)
	if !b.Ready() {
		t.Error("the non-owner is not ready after its second Start")
	}
}

// A non-owner's Start fails on a stream none of whose subjects captures
// every subject under its prefix, laid out as in
// TestNonOwnerBindsCapturingSubject.
func TestNonOwnerStartFailsOnUncapturedPrefix(t *testing.T) {
	url := natstest.Open(t)
	js := jetStream(t, url)
	for name, sfx := range map[string][]string{
		"narrower":           {".a.b.c.>"},
		"two narrower ones":  {".a.b.c.>", ".a.b.d.>"},
		"'*' after it":       {".a.b.*.>"},
		"one token after it": {".a.b.*"},
		"fixed subjects":     {".a.b.created", ".a.b.deleted"},
		"itself, fixed":      {".a.b"},
		"'*' for its last":   {".a.*"},
		"a sibling":          {".a.c.>"},
		"a longer token":     {".a.bb.>"},
	} {
		t.Run(name, func(t *testing.T) {
			stream, prefix := natstest.Scratch(t, url)
			var subjects []string
			for _, s := range sfx {
				subjects = append(subjects, prefix+s)
			}
			createStream(t, js, stream, subjects...)
			b := newConfigured(t, nats.Config{URL: url, Stream: stream, Prefix: prefix + ".a.b"})
			ctx, cancel := context.WithTimeout(t.Context(), failsafe)
			defer cancel()
			err := b.Start(ctx)
			if err == nil {
				shutdown(t, b)
				t.Fatalf("Start bound a stream on %q for prefix %s.a.b", subjects, prefix)
			}
			t.Logf("Start: %v", err)
			if b.Ready() || b.Conn() != nil {
				t.Error("a broker whose Start failed is ready, or has a connection")
			}
		})
	}
}

// A started broker starts once: a second Start fails with ErrStarted and
// leaves the first connection serving.
func TestSecondStartFails(t *testing.T) {
	url := natstest.Open(t)
	stream, prefix := natstest.Scratch(t, url)
	b := started(t, nats.Config{URL: url, Stream: stream, Prefix: prefix, Owner: new(true)})
	first := b.Conn()
	if err := b.Start(t.Context()); !errors.Is(err, nats.ErrStarted) {
		t.Errorf("second Start = %v, want ErrStarted", err)
	}
	if b.Conn() != first || !b.Ready() {
		t.Error("a second Start replaced or broke the first connection")
	}
}

// Publish sends an event in binary content mode to Prefix.<type>, keyed by
// its length-prefixed source and id, so a repeat within the window is
// dropped, and a source and id that spell the same concatenation are not.
func TestPublishDedups(t *testing.T) {
	url := natstest.Open(t)
	js := jetStream(t, url)
	stream, prefix := natstest.Scratch(t, url)
	b := started(t, nats.Config{URL: url, Stream: stream, Prefix: prefix, Owner: new(true)})

	e := event.Event{ID: "b1", Source: "/a", Type: "orders.created", DataContentType: "application/json", Data: []byte(`{"n":1}`)}
	publish(t, b, e)
	publish(t, b, e)
	if n := streamInfo(t, js, stream).State.Msgs; n != 1 {
		t.Errorf("after a repeat, stream holds %d messages, want 1", n)
	}
	publish(t, b, event.Event{ID: "1", Source: "/ab", Type: "orders.created"})
	if n := streamInfo(t, js, stream).State.Msgs; n != 2 {
		t.Errorf("after a distinct source and id, stream holds %d messages, want 2", n)
	}

	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	s, err := js.Stream(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	m, err := s.GetMsg(ctx, 1)
	if err != nil {
		t.Fatalf("get the first message: %v", err)
	}
	if want := prefix + ".orders.created"; m.Subject != want {
		t.Errorf("Subject = %q, want %q", m.Subject, want)
	}
	if got, want := m.Header.Get(jetstream.MsgIDHeader), "2:/ab1"; got != want {
		t.Errorf("Nats-Msg-Id = %q, want %q", got, want)
	}
	if got := m.Header.Get("ce-id"); got != "b1" {
		t.Errorf("ce-id = %q, want b1: the event in binary content mode", got)
	}
	if string(m.Data) != `{"n":1}` {
		t.Errorf("Data = %s, want the event's data", m.Data)
	}
}
