//go:build integration

package nats_test

import (
	"context"
	"errors"
	"iter"
	"strings"
	"testing"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/standards-lab/go-core/event"

	"github.com/standards-lab/go-messaging"
	"github.com/standards-lab/go-messaging/messagingtest"
	"github.com/standards-lab/go-messaging/nats"
	"github.com/standards-lab/go-messaging/nats/natstest"
)

func TestJournalConformance(t *testing.T) {
	url := natstest.Open(t)
	messagingtest.RunJournal(t, func(t *testing.T) namespaced {
		if strings.Contains(t.Name(), "/Read") {
			t.Skip("nats.Broker has no Read yet")
		}
		stream, prefix := natstest.Scratch(t, url)
		b := started(t, nats.Config{URL: url, Stream: stream, Prefix: prefix, Owner: new(true)})
		return namespaced{Broker: b, prefix: prefix}
	})
}

func (n namespaced) Append(ctx context.Context, subject string, expect uint64, e event.Event) (uint64, error) {
	if subject != "" {
		subject = n.prefix + "." + subject
	}
	return n.Broker.Append(ctx, subject, expect, e)
}

func (n namespaced) Read(context.Context, string, uint64) iter.Seq2[messaging.Entry, error] {
	return func(yield func(messaging.Entry, error) bool) {
		yield(messaging.Entry{}, errors.New("nats: Read is not implemented"))
	}
}

func TestAppendRetryAfterLostAck(t *testing.T) {
	b, url, stream, prefix := ownerBroker(t)
	js := jetStream(t, url)
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	subject := prefix + ".journal"
	e := event.Event{ID: "1", Source: "/test", Type: prefix + ".created"}

	first, err := b.Append(ctx, subject, 0, e)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	retry := natsgo.NewMsg(subject)
	retry.Header.Set(jetstream.MsgIDHeader, "5:/test1")
	if _, err := js.PublishMsg(ctx, retry, jetstream.WithExpectLastSequencePerSubject(0)); !nats.WrongLastSequence(err) {
		t.Fatalf("the server's answer to the retry = %v, want a wrong last sequence: only then does Append's lookup decide the retry", err)
	}
	got, err := b.Append(ctx, subject, 0, e)
	if err != nil {
		t.Fatalf("the retry: %v, want the first append's sequence", err)
	}
	if got != first {
		t.Errorf("the retry returned %d, want the first append's sequence %d", got, first)
	}
	if n := streamInfo(t, js, stream).State.Msgs; n != 1 {
		t.Errorf("stream holds %d messages, want the one appended", n)
	}
}
