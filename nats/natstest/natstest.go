package natstest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Open returns the NATS server's URL, from MESSAGING_NATS_URL. A missing
// MESSAGING_NATS_URL fails the test: the integration tag states that the
// stack is expected.
func Open(t testing.TB) string {
	t.Helper()
	url := os.Getenv("MESSAGING_NATS_URL")
	if url == "" {
		t.Fatal("MESSAGING_NATS_URL is not set; run under mise with the compose stack up")
	}
	return url
}

// Scratch returns a stream name, messaging_test_<hex>, and a type
// namespace, messaging.test.<hex>, unique to the test, which pass
// messaging.CheckName and messaging.CheckType, as a broker's Config
// requires of its Stream and Prefix. A broker publishes an event with its
// type as the subject, so the test's event types are under the namespace:
// prefix+".thing.happened". It creates nothing. When the test ends it
// deletes the stream on url's server, tolerating one that nothing created.
func Scratch(t testing.TB, url string) (stream, prefix string) {
	t.Helper()
	sfx := suffix(t)
	stream, prefix = "messaging_test_"+sfx, "messaging.test."+sfx
	t.Cleanup(func() {
		nc, err := natsgo.Connect(url, natsgo.Name(t.Name()))
		if err != nil {
			t.Errorf("delete stream %s: connect %s: %v", stream, url, err)
			return
		}
		defer nc.Close()
		js, err := jetstream.New(nc)
		if err != nil {
			t.Errorf("delete stream %s: %v", stream, err)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := js.DeleteStream(ctx, stream); err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
			t.Errorf("delete stream %s: %v", stream, err)
		}
	})
	return stream, prefix
}

func suffix(t testing.TB) string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("random suffix: %v", err)
	}
	return hex.EncodeToString(b[:])
}

// ScratchQueue returns a stream name and a work queue's work and dead-letter
// subjects, messaging.test.<hex>.jobs and messaging.test.<hex>.dead, unique
// to the test, as a queue's QueueConfig requires of its Stream, Subject,
// and DeadLetter. Like [Scratch], it creates nothing, and when the test
// ends it deletes the stream on url's server, tolerating one that nothing
// created.
func ScratchQueue(t testing.TB, url string) (stream, subject, deadLetter string) {
	t.Helper()
	stream, prefix := Scratch(t, url)
	return stream, prefix + ".jobs", prefix + ".dead"
}
