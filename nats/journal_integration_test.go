//go:build integration

package nats_test

import (
	"context"
	"iter"
	"strconv"
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

func (n namespaced) Read(ctx context.Context, subject string, after uint64) iter.Seq2[messaging.Entry, error] {
	if subject != "" {
		subject = n.prefix + "." + subject
	}
	return n.Broker.Read(ctx, subject, after)
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
	if ack, err := js.PublishMsg(ctx, retry, jetstream.WithExpectLastSequencePerSubject(0)); err == nil {
		t.Fatalf("the server acknowledged the retry (duplicate %t), want a wrong last sequence: only then does Append's lookup decide the retry", ack.Duplicate)
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

func consumers(t *testing.T, js jetstream.JetStream, stream string) int {
	t.Helper()
	return streamInfo(t, js, stream).State.Consumers
}

func TestReadYieldsUndecodableAndStops(t *testing.T) {
	b, url, stream, prefix := ownerBroker(t)
	js := jetStream(t, url)
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	subject := prefix + ".journal"
	first, err := b.Append(ctx, subject, 0, event.Event{ID: "1", Source: "/test", Type: prefix + ".t"})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	ack, err := js.Publish(ctx, subject, []byte("not an event"))
	if err != nil {
		t.Fatalf("publish raw bytes: %v", err)
	}
	if _, err := b.Append(ctx, subject, ack.Sequence, event.Event{ID: "3", Source: "/test", Type: prefix + ".t"}); err != nil {
		t.Fatalf("Append after the raw message: %v", err)
	}
	var got []messaging.Entry
	var errs []error
	for entry, err := range b.Read(ctx, subject, 0) {
		if err != nil {
			errs = append(errs, err)
			continue
		}
		got = append(got, entry)
	}
	if len(got) != 1 || got[0].Sequence != first {
		t.Errorf("read %+v, want the event before the undecodable message only", got)
	}
	if len(errs) != 1 {
		t.Fatalf("read errors %v, want one: the undecodable message ends the read", errs)
	}
	t.Logf("yielded: %v", errs[0])
	eventually(t, "the read's consumer to be released", func() bool { return consumers(t, js, stream) == 0 })
}

func TestReadReleasesConsumer(t *testing.T) {
	b, url, stream, prefix := ownerBroker(t)
	js := jetStream(t, url)
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	subject := prefix + ".journal"
	var last uint64
	for _, id := range []string{"1", "2", "3"} {
		seq, err := b.Append(ctx, subject, last, event.Event{ID: id, Source: "/test", Type: prefix + ".t"})
		if err != nil {
			t.Fatalf("Append %s: %v", id, err)
		}
		last = seq
	}
	var read int
	for entry, err := range b.Read(ctx, subject, 0) {
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		read++
		if entry.Sequence == last && read != 3 {
			t.Errorf("read the last event as entry %d, want 3", read)
		}
	}
	if read != 3 {
		t.Errorf("read %d entries, want 3", read)
	}
	eventually(t, "the read's consumer to be released at its end", func() bool { return consumers(t, js, stream) == 0 })
	for _, err := range b.Read(ctx, subject, 0) {
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if n := consumers(t, js, stream); n != 1 {
			t.Fatalf("the stream has %d consumers during the read, want the read's one", n)
		}
		break
	}
	eventually(t, "the read's consumer to be released after the break", func() bool { return consumers(t, js, stream) == 0 })
}

func TestReadCreatesNoConsumerWhenNothingToRead(t *testing.T) {
	b, url, stream, prefix := ownerBroker(t)
	js := jetStream(t, url)
	ctx, cancel := context.WithTimeout(t.Context(), failsafe)
	defer cancel()
	subject := prefix + ".journal"
	last, err := b.Append(ctx, subject, 0, event.Event{ID: "1", Source: "/test", Type: prefix + ".t"})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	created, err := js.Conn().SubscribeSync("$JS.EVENT.ADVISORY.CONSUMER.CREATED." + stream + ".*")
	if err != nil {
		t.Fatalf("subscribe to consumer advisories: %v", err)
	}
	if err := js.Conn().Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	for _, c := range []struct {
		name    string
		subject string
		after   uint64
	}{
		{"a subject with no events", prefix + ".empty", 0},
		{"after the last sequence", subject, last},
		{"beyond the last sequence", subject, last + 5},
	} {
		for entry, err := range b.Read(ctx, c.subject, c.after) {
			t.Errorf("%s: read yielded %+v, %v; want nothing", c.name, entry, err)
		}
	}
	if _, err := js.CreateConsumer(ctx, stream, jetstream.ConsumerConfig{Durable: "marker"}); err != nil {
		t.Fatalf("create the marker consumer: %v", err)
	}
	for {
		msg, err := created.NextMsg(failsafe)
		if err != nil {
			t.Fatalf("the marker consumer's advisory: %v", err)
		}
		if strings.HasSuffix(msg.Subject, ".marker") {
			break
		}
		t.Errorf("a read created consumer %s", msg.Subject)
	}
}

func TestReadEndsWhenItsBoundIsDeleted(t *testing.T) {
	for _, c := range []struct {
		name   string
		during bool
	}{
		{"an event appended during the read", true},
		{"nothing after it", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			b, url, stream, prefix := ownerBroker(t)
			js := jetStream(t, url)
			ctx, cancel := context.WithTimeout(t.Context(), failsafe)
			defer cancel()
			s, err := js.Stream(ctx, stream)
			if err != nil {
				t.Fatalf("stream %s: %v", stream, err)
			}
			subject := prefix + ".journal"
			ev := func(id string) event.Event { return event.Event{ID: id, Source: "/test", Type: prefix + ".t"} }
			var first, bound uint64
			for i := range 5000 {
				seq, err := b.Append(ctx, subject, bound, ev(strconv.Itoa(i)))
				if err != nil {
					t.Fatalf("Append %d: %v", i, err)
				}
				if i == 0 {
					first = seq
				}
				bound = seq
			}
			var read int
			for entry, err := range b.Read(ctx, subject, 0) {
				if err != nil {
					t.Fatalf("Read after %d entries: %v", read, err)
				}
				read++
				if entry.Sequence >= bound {
					t.Errorf("read %s at %d, want nothing at or past the deleted bound %d", entry.Event.ID, entry.Sequence, bound)
				}
				if entry.Sequence != first {
					continue
				}
				if err := s.DeleteMsg(ctx, bound); err != nil {
					t.Fatalf("delete the bound %d: %v", bound, err)
				}
				if c.during {
					if _, err := b.Append(ctx, subject, bound-1, ev("during")); err != nil {
						t.Fatalf("Append during the read: %v", err)
					}
				}
			}
			if read != 4999 {
				t.Errorf("read %d entries, want the 4999 before the deleted bound", read)
			}
		})
	}
}
