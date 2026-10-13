//go:build integration

package nats_test

import (
	"context"
	"testing"
	"time"

	"github.com/standards-lab/go-messaging/nats"
	"github.com/standards-lab/go-messaging/nats/natstest"
)

// TestBrokerStarts proves Start connects to the compose server, and that
// Shutdown drains and closes the connection it opened.
func TestBrokerStarts(t *testing.T) {
	url := natstest.Open(t)
	stream, prefix := natstest.Scratch(t, url)
	b, err := nats.New(nats.Config{URL: url, Name: t.Name(), Stream: stream, Prefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
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
