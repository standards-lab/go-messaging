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
// passes once a pass finds no row. poll and any [Timeout] must be positive,
// and any [Drain] not negative.
//
// The relay passes each error it survives to report, with the context it
// was receiving under: each row it quarantines, as a [*QuarantineError];
// each pass a transient publish failure ends, as a [*PublishError]; and
// each database error. A nil report discards them. report has
// messaging.Report's shape, so a messaging.Report can be given. It must not
// block for long, since the relay waits on it before its next row.
func (o *Outbox) Relay(db sqlate.Beginner, poll time.Duration, report func(ctx context.Context, err error), opts ...RelayOption) *Relay {
	if report == nil {
		report = func(context.Context, error) {}
	}
	r := &Relay{eng: o.eng, db: db, poll: poll, timeout: defaultTimeout, report: report}
	for _, opt := range opts {
		opt(r)
	}
	if r.poll <= 0 || r.timeout <= 0 || r.drain < 0 {
		panic("outbox: relay poll and timeout must be positive, and drain not negative")
	}
	return r
}
