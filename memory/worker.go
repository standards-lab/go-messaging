package memory

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-core/reactor"
	"github.com/standards-lab/go-messaging"
)

type member struct {
	q      *Queue
	w      messaging.Worker
	report messaging.Report
	ready  atomic.Bool
}

func (m *member) Receive(ctx context.Context, fn reactor.Func[event.Event]) error {
	m.ready.Store(true)
	defer m.ready.Store(false)
	for ctx.Err() == nil {
		hctx, cancel := context.WithCancel(ctx)
		m.q.mu.Lock()
		now := m.q.clock.now()
		j, c, attempt, ok := m.q.take(m.w, cancel, now)
		var wake <-chan struct{}
		var due <-chan time.Time
		stop := func() {}
		if !ok {
			wake = m.q.wake
			due, stop = m.q.clock.at(m.q.nextDue(m.w.Subject, now))
		}
		m.q.mu.Unlock()

		if !ok {
			cancel()
			select {
			case <-ctx.Done():
			case <-wake:
			case <-due:
			}
			stop()
			continue
		}
		m.handle(ctx, hctx, j, c, attempt, fn)
		cancel()
	}
	return nil
}

func (m *member) handle(ctx, hctx context.Context, j *job, c *claim, attempt int, fn reactor.Func[event.Event]) {
	e, err := event.Decode(j.header, j.body)
	if err != nil {
		m.report(ctx, fmt.Errorf("memory: %s: %w", m.w.Name, err))
		m.settle(j, c, event.Permanent(err), false)
		return
	}
	hctx = messaging.WithDeliveryEnd(messaging.WithAttempt(hctx, attempt), hctx.Done())
	if m.w.Subject != "" {
		if d, err := messaging.ReadDeadLetter(j.header); err == nil {
			hctx = messaging.WithDeadLetter(hctx, d)
		}
	}
	done := make(chan struct{})
	watched := make(chan struct{})
	go func() {
		defer close(watched)
		m.watch(j, c, done)
	}()
	err = fn(hctx, e)
	close(done)
	<-watched
	m.settle(j, c, err, ctx.Err() != nil)
}

func (m *member) watch(j *job, c *claim, done <-chan struct{}) {
	for {
		m.q.mu.Lock()
		m.q.expire(m.q.clock.now())
		if j.claim != c {
			m.q.mu.Unlock()
			return
		}
		var at time.Time
		if !c.extending {
			at = c.deadline
		}
		wake := m.q.wake
		due, stop := m.q.clock.at(at)
		m.q.mu.Unlock()
		select {
		case <-done:
			stop()
			return
		case <-wake:
		case <-due:
		}
		stop()
	}
}

func (m *member) settle(j *job, c *claim, err error, drained bool) {
	m.q.mu.Lock()
	defer m.q.mu.Unlock()
	m.q.settle(m.w, j, c, err, drained, m.q.clock.now())
}

func (m *member) Ready() bool { return m.ready.Load() }
