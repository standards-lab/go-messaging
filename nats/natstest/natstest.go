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

// Scratch returns a stream name, messaging_test_<hex>, and a subject
// prefix, messaging.test.<hex>, unique to the test, which pass
// messaging.CheckName and messaging.CheckType, as a broker's Config
// requires. It creates nothing. When the test ends it deletes the stream on
// url's server, tolerating one that nothing created.
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

// suffix returns eight random hex characters, so parallel packages never
// pick the same stream.
func suffix(t testing.TB) string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("random suffix: %v", err)
	}
	return hex.EncodeToString(b[:])
}
