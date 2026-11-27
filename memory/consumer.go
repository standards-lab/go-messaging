package memory

import (
	"time"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-messaging"
)

// See docs/internals.md
type consumer struct {
	sub     messaging.Subscription
	cursor  int
	pending map[int]delivery
	retries []retry
	counts  map[int]int
	tokens  uint64
	wake    chan struct{}
}

type delivery struct {
	token    uint64
	deadline time.Time
	attempt  int
}

type retry struct {
	seq int
	due time.Time
}

func newConsumer(sub messaging.Subscription) *consumer {
	return &consumer{
		sub:     sub,
		pending: map[int]delivery{},
		counts:  map[int]int{},
		wake:    make(chan struct{}),
	}
}

func (c *consumer) wakeAll() {
	close(c.wake)
	c.wake = make(chan struct{})
}

func (c *consumer) take(log []message, now time.Time) (seq int, d delivery, ok bool, wake <-chan struct{}, wait time.Duration) {
	c.expire(now)
	pick := -1
	for i, r := range c.retries {
		if !now.Before(r.due) && (pick < 0 || r.due.Before(c.retries[pick].due)) {
			pick = i
		}
	}
	switch {
	case pick >= 0:
		seq = c.retries[pick].seq
		c.retries = append(c.retries[:pick], c.retries[pick+1:]...)
	default:
		for c.cursor < len(log) && !c.sub.Matches(log[c.cursor].subject) {
			c.cursor++
		}
		if c.cursor == len(log) {
			return 0, delivery{}, false, c.wake, c.nextDue(now)
		}
		seq = c.cursor
		c.cursor++
	}
	c.counts[seq]++
	c.tokens++
	d = delivery{token: c.tokens, deadline: now.Add(c.sub.AckWait), attempt: c.counts[seq]}
	c.pending[seq] = d
	return seq, d, true, nil, 0
}

func (c *consumer) settle(seq int, d delivery, err error, now time.Time) {
	c.expire(now)
	if cur, ok := c.pending[seq]; !ok || cur.token != d.token {
		return
	}
	delete(c.pending, seq)
	if err == nil || event.IsPermanent(err) {
		delete(c.counts, seq)
		return
	}
	c.redeliver(seq, now.Add(c.sub.RetryDelay))
}

func (c *consumer) expire(now time.Time) {
	for seq, d := range c.pending {
		if !now.Before(d.deadline) {
			delete(c.pending, seq)
			c.redeliver(seq, now)
		}
	}
}

func (c *consumer) redeliver(seq int, due time.Time) {
	if c.sub.MaxDeliver > 0 && c.counts[seq] >= c.sub.MaxDeliver {
		delete(c.counts, seq)
		return
	}
	c.retries = append(c.retries, retry{seq: seq, due: due})
	c.wakeAll()
}

func (c *consumer) nextDue(now time.Time) time.Duration {
	var next time.Time
	for _, r := range c.retries {
		if next.IsZero() || r.due.Before(next) {
			next = r.due
		}
	}
	for _, d := range c.pending {
		if next.IsZero() || d.deadline.Before(next) {
			next = d.deadline
		}
	}
	if next.IsZero() {
		return 0
	}
	return max(next.Sub(now), time.Millisecond)
}
