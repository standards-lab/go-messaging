package messaging

import (
	"context"

	"github.com/standards-lab/go-core/reactor"
)

// Gate is a quiesce gate the runtime's reactors hold shared while they
// work, so an operation that holds it exclusively runs with them paused:
// go-core's [reactor.Gate] is one. Shared waits until the gate admits a
// shared holder and returns its release, or fails, holding nothing; a wait
// ctx ends returns ctx's error.
type Gate interface {
	Shared(ctx context.Context) (release func(), err error)
}

var _ Gate = (*reactor.Gate)(nil)

// Gated sets the gate the runtime's reactors hold shared. Without it, or
// with a nil gate, they hold none.
//
// The relay holds g for each outbox row, from before the row's transaction
// begins until the transaction commits or rolls back, so a relay waiting on
// g opens no transaction and claims no row. Its wait ends with the relay's
// stop, and its last pass's with that pass's bound, either of which claims
// nothing and reports nothing. Any other failure of g ends the pass and is
// reported as a [FailureRelay], leaving the relay ready, since it is not
// the database's.
//
// Each delivery a [Runtime.Consume] reactor handles holds g for the
// handler's whole call, and the handler is not called until it does. A
// wait that the drain ends hands the handler nothing and leaves the event
// unacknowledged, for the broker to redeliver. The wait is bounded by the
// delivery's AckWait like the handler is: a delivery that waits on g past
// AckWait is redelivered, and counts against MaxDeliver, so an event whose
// last delivery waits that long is given up unreported. Any other failure
// of g is the delivery's outcome, as a transient error of the handler's
// would be: the event is redelivered, and on its last delivery reported as
// a [FailureExhausted].
func Gated(g Gate) Option {
	return func(r *Runtime) {
		if g != nil {
			r.gate = g
		}
	}
}
