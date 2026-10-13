package memory

import (
	"time"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-messaging"
)

// consumer is one durable consumer's delivery state machine. Every method
// runs with Broker.mu held, and is given the time rather than reading the
// clock. An event moves between three places:
//
//	cursor ──take──▶ pending ──settle(nil or Permanent)──▶ done
//	                  │   ▲
//	settle(error) or  │   │ take, once due
//	expire (AckWait)  ▼   │
//	                 retries
//
// An event that leaves pending by settle(error) or expire is terminated
// instead of retried once MaxDeliver deliveries have been made.
type consumer struct {
	sub     messaging.Subscription
	cursor  int              // the next sequence never delivered
	pending map[int]delivery // sequence to its delivery in flight
	retries []retry          // redeliveries waiting to come due
	counts  map[int]int      // sequence to deliveries made, while unsettled
	tokens  uint64
	wake    chan struct{} // closed when there may be new work
}

// delivery is one handling of an event. Its token tells a current outcome
// from a late one, and its attempt is the event's delivery count with it.
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

// wakeAll releases every member waiting for work. Closing the channel is the
// broadcast; a member captures the current channel under the lock, so it
// cannot miss a wake between looking for work and waiting.
func (c *consumer) wakeAll() {
	close(c.wake)
	c.wake = make(chan struct{})
}

// take claims the next event for delivery: a due retry first, then the next
// matching event in log past the cursor. With nothing to take, it returns
// the channel that signals new work and how long until a retry comes due or
// a delivery expires, or 0 when nothing is scheduled.
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
		for c.cursor < len(log) && !c.sub.Matches(log[c.cursor].typ) {
			c.cursor++ // acknowledged on sight
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

// settle applies a handler's outcome to the delivery it came from. An
// outcome for a delivery that expired is ignored: the event has been, or
// will be, redelivered. Expiring first matters for a lone member: no other
// member's take call sweeps its overrun.
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

// expire moves each delivery past its AckWait to the retries, due now, so
// any late outcome is ignored.
func (c *consumer) expire(now time.Time) {
	for seq, d := range c.pending {
		if !now.Before(d.deadline) {
			delete(c.pending, seq)
			c.redeliver(seq, now)
		}
	}
}

// redeliver schedules seq for another delivery at due, or terminates it
// once MaxDeliver deliveries have been made.
func (c *consumer) redeliver(seq int, due time.Time) {
	if c.sub.MaxDeliver > 0 && c.counts[seq] >= c.sub.MaxDeliver {
		delete(c.counts, seq)
		return
	}
	c.retries = append(c.retries, retry{seq: seq, due: due})
	c.wakeAll()
}

// nextDue is how long until the earliest retry comes due or delivery
// expires, so a waiting member wakes in time to act on it; 0 means nothing
// is scheduled.
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
