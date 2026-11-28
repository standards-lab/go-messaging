package memory

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-core/reactor"
	"github.com/standards-lab/go-messaging"
)

// Queue is an in-memory [messaging.Queue]. Its zero value is not usable;
// call [NewQueue].
//
// One mutex guards every job, so the job methods never lock; the queue and
// its sources take mu around them.
type Queue struct {
	clock clock
	mu    sync.Mutex
	jobs  []*job
	seen  map[slot]time.Time
	wake  chan struct{}
	keep  bool
}

type slot struct{ subject, source, id string }

// See docs/internals.md
type job struct {
	slot     slot
	header   event.Header
	body     []byte
	attempts int
	due      time.Time
	claim    *claim
}

type claim struct {
	ackWait   time.Duration
	deadline  time.Time
	extending bool
	cancel    context.CancelFunc
}

// NewQueue returns an empty queue.
func NewQueue() *Queue {
	return newQueue(wall{})
}

func newQueue(c clock) *Queue {
	return &Queue{clock: c, seen: map[slot]time.Time{}, wake: make(chan struct{})}
}

var _ messaging.Queue = (*Queue)(nil)

// Enqueue stores e as a job on the queue's own subject, as
// [messaging.Queue] describes. Its deduplication window is
// [messaging.DefaultDuplicates].
func (q *Queue) Enqueue(_ context.Context, e event.Event) error {
	h, body, err := messaging.Encode(e)
	if err != nil {
		return fmt.Errorf("memory: enqueue: %w", err)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.store(slot{source: e.Source, id: e.ID}, h, body)
	return nil
}

// Subscribe returns a source of w's events, as [messaging.Queue] describes.
// Every source on a subject takes from the same jobs, and each delivery is
// governed by the Worker of the source that took it. The source passes
// each job it cannot decode to report and dead-letters it.
func (q *Queue) Subscribe(w messaging.Worker, report messaging.Report) (reactor.Source[event.Event], error) {
	if err := w.Validate(); err != nil {
		return nil, err
	}
	if report == nil {
		report = func(context.Context, error) {}
	}
	return &member{q: q, w: w.Normalize(), report: report}, nil
}

func (q *Queue) wakeAll() {
	close(q.wake)
	q.wake = make(chan struct{})
}

func (q *Queue) store(s slot, h event.Header, body []byte) {
	now := q.clock.now()
	for k, at := range q.seen {
		if now.Sub(at) >= messaging.DefaultDuplicates {
			delete(q.seen, k)
		}
	}
	if _, ok := q.seen[s]; ok {
		return
	}
	q.seen[s] = now
	q.jobs = append(q.jobs, &job{slot: s, header: h, body: body})
	q.wakeAll()
}

func (q *Queue) remove(j *job) {
	q.jobs = slices.DeleteFunc(q.jobs, func(v *job) bool { return v == j })
}

func (q *Queue) take(w messaging.Worker, cancel context.CancelFunc, now time.Time) (*job, *claim, int, bool) {
	q.expire(now)
	for {
		j := q.next(w.Subject, now)
		if j == nil {
			return nil, nil, 0, false
		}
		if j.attempts >= w.MaxDeliver {
			q.deadLetter(j, w, messaging.DeadLetter{Attempts: j.attempts}, now)
			continue
		}
		j.attempts++
		j.claim = &claim{ackWait: w.AckWait, deadline: now.Add(w.AckWait), extending: true, cancel: cancel}
		return j, j.claim, j.attempts, true
	}
}

func (q *Queue) next(subject string, now time.Time) *job {
	var pick *job
	for _, j := range q.jobs {
		if j.slot.subject != subject || j.claim != nil || j.due.After(now) {
			continue
		}
		switch {
		case pick == nil:
			pick = j
		case j.attempts > 0 && (pick.attempts == 0 || j.due.Before(pick.due)):
			pick = j
		}
	}
	return pick
}

func (q *Queue) nextDue(subject string, now time.Time) time.Time {
	var next time.Time
	for _, j := range q.jobs {
		if j.slot.subject != subject {
			continue
		}
		at := j.due
		if c := j.claim; c != nil {
			if c.extending {
				continue
			}
			at = c.deadline
		}
		if at.After(now) && (next.IsZero() || at.Before(next)) {
			next = at
		}
	}
	return next
}

func (q *Queue) expire(now time.Time) {
	for _, j := range q.jobs {
		c := j.claim
		switch {
		case c == nil:
		case c.extending:
			c.deadline = now.Add(c.ackWait)
		case !now.Before(c.deadline):
			c.cancel()
			j.claim = nil
			j.due = now
			q.wakeAll()
		}
	}
}

func (q *Queue) settle(w messaging.Worker, j *job, c *claim, err error, drained bool, now time.Time) {
	q.expire(now)
	if j.claim != c {
		return
	}
	j.claim = nil
	switch {
	case err == nil:
		q.remove(j)
	case drained && !event.IsPermanent(err):
		j.due = now
	case event.IsPermanent(err) || j.attempts >= w.MaxDeliver:
		q.deadLetter(j, w, messaging.DeadLetter{Attempts: j.attempts, Errored: true, Err: err.Error()}, now)
	default:
		j.due = now.Add(w.RetryDelay(j.attempts))
	}
	q.wakeAll()
}

func (q *Queue) deadLetter(j *job, w messaging.Worker, d messaging.DeadLetter, now time.Time) {
	h := maps.Clone(j.header)
	maps.Copy(h, d.Header())
	q.store(slot{w.DeadLetter, j.slot.source, j.slot.id}, h, j.body)
	if q.keep {
		q.keep = false
		j.due = now
		return
	}
	q.remove(j)
}

type clock interface {
	now() time.Time
	at(t time.Time) (<-chan time.Time, func())
}

type wall struct{}

func (wall) now() time.Time { return time.Now() }

func (wall) at(t time.Time) (<-chan time.Time, func()) {
	if t.IsZero() {
		return nil, func() {}
	}
	tm := time.NewTimer(time.Until(t))
	return tm.C, func() { tm.Stop() }
}
