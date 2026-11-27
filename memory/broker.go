package memory

import (
	"context"
	"fmt"
	"iter"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-core/reactor"
	"github.com/standards-lab/go-messaging"
)

// Broker is an in-memory [messaging.Broker] and [messaging.Journal]. Its
// zero value is not usable; call [New].
//
// One mutex guards the log and every consumer, so the consumer's methods
// never lock; the broker and the sources take mu around them.
type Broker struct {
	mu        sync.Mutex
	log       []message
	consumers map[string]*consumer
	seen      map[identity]seen
}

type identity struct{ source, id string }

type seen struct {
	at  time.Time
	seq uint64
}

type message struct {
	subject string
	id      identity
	header  event.Header
	body    []byte
}

// New returns an empty broker.
func New() *Broker {
	return &Broker{consumers: map[string]*consumer{}, seen: map[identity]seen{}}
}

var (
	_ messaging.Broker  = (*Broker)(nil)
	_ messaging.Journal = (*Broker)(nil)
)

// Publish appends e to the log on its type's subject and wakes every
// consumer. When an event with e's source and id was published or appended
// within [messaging.DefaultDuplicates], Publish instead drops e and returns
// nil.
func (b *Broker) Publish(_ context.Context, e event.Event) error {
	h, body, err := messaging.Encode(e)
	if err != nil {
		return fmt.Errorf("memory: publish: %w", err)
	}
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	key := identity{e.Source, e.ID}
	if _, ok := b.duplicate(key, now); ok {
		return nil
	}
	b.seen[key] = seen{at: now, seq: b.add(message{subject: e.Type, id: key, header: h, body: body})}
	return nil
}

// Append appends e to the log on subject, as [messaging.Journal] describes,
// and wakes every consumer. Its log is numbered like a stream, from 1, and
// shared with Publish, as is its deduplication window,
// [messaging.DefaultDuplicates].
func (b *Broker) Append(_ context.Context, subject string, expect uint64, e event.Event) (uint64, error) {
	h, body, err := messaging.Encode(e)
	if err == nil {
		err = carriable(h)
	}
	if err == nil {
		if err = messaging.CheckType(subject); err != nil {
			err = fmt.Errorf("subject: %w", err)
		}
	}
	if err != nil {
		return 0, fmt.Errorf("memory: append: %w", err)
	}
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	key := identity{e.Source, e.ID}
	if seq, ok := b.duplicate(key, now); ok {
		return seq, nil
	}
	if last := b.last(subject); last != expect {
		if last > 0 && b.log[last-1].id == key {
			return last, nil
		}
		return 0, fmt.Errorf("memory: append %q at %d, last %d: %w", subject, expect, last, messaging.ErrConflict)
	}
	seq := b.add(message{subject: subject, id: key, header: h, body: body})
	b.seen[key] = seen{at: now, seq: seq}
	return seq, nil
}

// Read returns subject's events after the stream sequence after, as
// [messaging.Journal] describes. Each iteration takes the log as it stands
// when it begins, so it holds nothing while it yields.
func (b *Broker) Read(_ context.Context, subject string, after uint64) iter.Seq2[messaging.Entry, error] {
	return func(yield func(messaging.Entry, error) bool) {
		b.mu.Lock()
		log := b.log
		b.mu.Unlock()
		for i := after; i < uint64(len(log)); i++ {
			m, seq := log[i], i+1
			if m.subject != subject {
				continue
			}
			e, err := event.Decode(m.header, m.body)
			if err != nil {
				yield(messaging.Entry{}, fmt.Errorf("memory: read %q: message %d: %w", subject, seq, err))
				return
			}
			if !yield(messaging.Entry{Event: e, Sequence: seq}, nil) {
				return
			}
		}
	}
}

func (b *Broker) duplicate(key identity, now time.Time) (uint64, bool) {
	for id, s := range b.seen {
		if now.Sub(s.at) >= messaging.DefaultDuplicates {
			delete(b.seen, id)
		}
	}
	s, ok := b.seen[key]
	return s.seq, ok
}

func (b *Broker) last(subject string) uint64 {
	for i, v := range slices.Backward(b.log) {
		if v.subject == subject {
			return uint64(i + 1)
		}
	}
	return 0
}

func (b *Broker) add(m message) uint64 {
	b.log = append(b.log, m)
	for _, c := range b.consumers {
		c.wakeAll()
	}
	return uint64(len(b.log))
}

func carriable(h event.Header) error {
	for name, vs := range h {
		for _, v := range vs {
			if strings.ContainsAny(v, "\r\n") {
				return fmt.Errorf("header %s: a value must not contain CR or LF", name)
			}
		}
	}
	return nil
}

// Subscribe returns a source on the durable consumer that sub names,
// creating it on first use, at the log's beginning or, under
// [messaging.StartNew], at its end. Sources under one Name share its position
// and split its work. A later subscription under an existing Name must match
// the first, as binding to a JetStream durable must. The source passes each
// message it cannot decode to report and terminates it.
func (b *Broker) Subscribe(sub messaging.Subscription, report messaging.Report) (reactor.Source[event.Event], error) {
	if err := sub.Validate(); err != nil {
		return nil, err
	}
	sub = sub.Normalize()
	b.mu.Lock()
	defer b.mu.Unlock()
	c, ok := b.consumers[sub.Name]
	if !ok {
		c = newConsumer(sub)
		if sub.Start == messaging.StartNew {
			c.cursor = len(b.log)
		}
		b.consumers[sub.Name] = c
	} else if !reflect.DeepEqual(c.sub, sub) {
		return nil, fmt.Errorf("memory: subscription %q exists with a different configuration", sub.Name)
	}
	if report == nil {
		report = func(context.Context, error) {}
	}
	return &source{b: b, c: c, report: report}, nil
}
