package memory

import (
	"sync"
	"time"

	"github.com/standards-lab/go-core/event"
)

func (b *Broker) AppendRaw(subject string, h event.Header, body []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.add(message{subject: subject, header: h, body: body})
}

func (b *Broker) ForgetDuplicates() {
	b.mu.Lock()
	defer b.mu.Unlock()
	clear(b.seen)
}

type Clock struct {
	mu     sync.Mutex
	t      time.Time
	timers map[*timer]struct{}
}

type timer struct {
	at time.Time
	ch chan time.Time
}

func NewQueueOnClock() (*Queue, *Clock) {
	c := &Clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), timers: map[*timer]struct{}{}}
	return newQueue(c), c
}

func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
	for tm := range c.timers {
		if !tm.at.After(c.t) {
			tm.ch <- c.t
			delete(c.timers, tm)
		}
	}
}

func (c *Clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *Clock) at(t time.Time) (<-chan time.Time, func()) {
	if t.IsZero() {
		return nil, func() {}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	tm := &timer{at: t, ch: make(chan time.Time, 1)}
	if !t.After(c.t) {
		tm.ch <- c.t
		return tm.ch, func() {}
	}
	c.timers[tm] = struct{}{}
	return tm.ch, func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		delete(c.timers, tm)
	}
}

func (q *Queue) LoseExtension() {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.clock.now()
	for _, j := range q.jobs {
		if c := j.claim; c != nil && c.extending {
			c.extending = false
			c.deadline = now.Add(c.ackWait)
		}
	}
	q.wakeAll()
}

func (q *Queue) KeepNextDeadLettered() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.keep = true
}
