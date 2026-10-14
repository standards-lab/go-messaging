//go:build integration

package natstest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/standards-lab/go-messaging"
	"github.com/standards-lab/go-messaging/nats/natstest"
)

// TestScratch proves Scratch's names are unique and usable, that it creates
// no stream itself, and that its cleanup deletes a stream created under its
// name and tolerates none being created.
func TestScratch(t *testing.T) {
	url := natstest.Open(t)
	js := jetStream(t, url)

	var created string
	if !t.Run("created", func(t *testing.T) {
		stream, prefix := natstest.Scratch(t, url)
		if err := messaging.CheckName(stream); err != nil {
			t.Errorf("stream %q: %v", stream, err)
		}
		if err := messaging.CheckType(prefix); err != nil {
			t.Errorf("prefix %q: %v", prefix, err)
		}
		if _, err := js.Stream(ctx(t), stream); !errors.Is(err, jetstream.ErrStreamNotFound) {
			t.Fatalf("stream %s before any create: err %v, want ErrStreamNotFound", stream, err)
		}
		if _, err := js.CreateStream(ctx(t), jetstream.StreamConfig{Name: stream, Subjects: []string{prefix + ".>"}}); err != nil {
			t.Fatalf("create stream %s: %v", stream, err)
		}
		created = stream
	}) {
		t.Fatal("Scratch with a stream created under its name failed")
	}
	if _, err := js.Stream(ctx(t), created); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Errorf("stream %s after cleanup: err %v, want ErrStreamNotFound", created, err)
	}

	var other string
	if !t.Run("not created", func(t *testing.T) {
		other, _ = natstest.Scratch(t, url)
	}) {
		t.Fatal("Scratch with no stream created failed its cleanup")
	}
	if other == created {
		t.Errorf("two tests got the same stream %s", other)
	}
}

// jetStream connects to url, closing the connection when the test ends.
func jetStream(t *testing.T, url string) jetstream.JetStream {
	t.Helper()
	nc, err := natsgo.Connect(url)
	if err != nil {
		t.Fatalf("connect %s: %v", url, err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	return js
}

// ctx is a context bounded to 10s, cancelled when the test ends.
func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}
