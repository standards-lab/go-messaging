package outbox

import (
	"context"
	"time"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/sqlate"
)

// Outbox runs the outbox on one engine's statements: the sink a recorder
// writes events through, and the relay that publishes them.
type Outbox struct {
	eng Engine
}

// New returns an outbox over eng, once eng defines every statement with the
// parameters [Engine] names.
func New(eng Engine) (*Outbox, error) {
	if err := eng.validate(); err != nil {
		return nil, err
	}
	return &Outbox{eng: eng}, nil
}

// Sink returns the [event.Sink] a composition root builds a service's
// [event.Recorder] over: it writes the events a command raises into the
// command's own transaction.
func (o *Outbox) Sink() event.Sink[*sqlate.Tx] { return sink{o.eng} }

// Relay returns a relay over the outbox in db that waits poll between
// passes once a pass finds no row, configured by opts. It panics when poll
// is not positive, or when opts' Timeout or Drain is negative.
func (o *Outbox) Relay(db sqlate.Beginner, poll time.Duration, opts RelayOptions) *Relay {
	if poll <= 0 || opts.Timeout < 0 || opts.Drain < 0 {
		panic("outbox: relay poll must be positive, and timeout and drain not negative")
	}
	r := &Relay{eng: o.eng, db: db, poll: poll, timeout: opts.Timeout, drain: opts.Drain, gate: opts.Gate, report: opts.Report}
	if r.timeout == 0 {
		r.timeout = defaultTimeout
	}
	if r.report == nil {
		r.report = func(context.Context, error) {}
	}
	return r
}
