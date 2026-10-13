package memory

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-core/reactor"
	"github.com/standards-lab/go-messaging"
)

// Broker is an in-memory [messaging.Broker]. Its zero value is not usable;
// call [New].
//
// One mutex guards the log and every consumer, so the consumer's methods
// never lock; the broker and the sources take mu around them.
type Broker struct {
	mu        sync.Mutex
	log       []message // an event's sequence is its index
	consumers map[string]*consumer
	seen      map[identity]time.Time // to when it was first published, within the window
}

// identity is what makes a CloudEvents event unique: its source and id.
type identity struct{ source, id string }

type message struct {
	typ    string // kept beside the header, so the type filter need not decode
	header event.Header
	body   []byte
}

// New returns an empty broker.
func New() *Broker {
	return &Broker{consumers: map[string]*consumer{}, seen: map[identity]time.Time{}}
}

var _ messaging.Broker = (*Broker)(nil)

// Publish appends e to the log and wakes every consumer. When an event with
// e's source and id was published within [messaging.DefaultDuplicates],
// Publish instead drops e and returns nil.
func (b *Broker) Publish(_ context.Context, e event.Event) error {
	h, body, err := messaging.Encode(e)
	if err != nil {
		return fmt.Errorf("memory: publish: %w", err)
	}
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	for id, at := range b.seen {
		if now.Sub(at) >= messaging.DefaultDuplicates {
			delete(b.seen, id)
		}
	}
	key := identity{e.Source, e.ID}
	if _, ok := b.seen[key]; ok {
		return nil
	}
	b.seen[key] = now
	b.log = append(b.log, message{typ: e.Type, header: h, body: body})
	for _, c := range b.consumers {
		c.wakeAll()
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
