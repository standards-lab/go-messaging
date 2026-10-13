package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-core/reactor"
	"github.com/standards-lab/sqlate"
	"github.com/standards-lab/sqlate/query"
)

// defaultTimeout bounds the handling of one row unless [Timeout] sets it.
const defaultTimeout = 10 * time.Second

// RelayOption configures a Relay.
type RelayOption func(*Relay)

// Timeout sets the deadline on each row's handler context. The row stays
// locked while its handler runs, so the timeout bounds how long a hung
// publish holds it. The row's whole transaction is bounded at twice the
// timeout, so a stalled database, or a handler that ignores its deadline,
// rolls the transaction back rather than hold the row and the drain. It
// must be positive.
func Timeout(d time.Duration) RelayOption {
	return func(r *Relay) { r.timeout = d }
}

// Drain sets how long a relay keeps publishing once it is cancelled. The
// relay makes one last pass on a context of its own, bounded by d. The pass
// returns when the outbox is empty or a row fails, so the events committed
// while the producers drained are published before the process exits rather
// than at its next start. Once d runs out, the pass claims no further row.
// A row already claimed settles on its own deadlines, as every row does: its
// handler's [Timeout] and its transaction's twice that. The reactor's
// [reactor.CancelAfter] cancels the handler first if it runs out sooner, so
// set d below it. The default, 0, makes no last pass. d must not be
// negative.
func Drain(d time.Duration) RelayOption {
	return func(r *Relay) { r.drain = d }
}

// Relay is the source of the outbox's committed, unpublished events. A
// composition root runs it into a reactor whose handler publishes each
// event to the broker.
//
// Each row is handled in a transaction of its own. The relay locks the
// oldest row neither published nor quarantined, skipping rows another relay
// holds, and calls the handler with its event inside that transaction, so
// the row stays locked while the event is published. When the handler
// returns nil, the row is marked published in the same transaction. When it
// returns an error marked [event.Permanent], the row is quarantined in the
// same transaction instead, and the pass goes on to the next row; so it does
// for a row whose header or event does not decode, without calling the
// handler. When the handler returns any other error, the transaction rolls
// back, the row stays unpublished, and the pass ends. The next pass, a poll
// later, retries it, without bound. A stop anywhere before the commit leaves
// the row to be published again under the same source and id, so delivery
// is at least once and a broker that deduplicates on them sees it once.
//
// One relay publishes in seq order, which is the order the rows were
// inserted among those committed, not the order their transactions
// committed: a long transaction can commit a row after a later one was
// published. It holds the rest of the outbox back behind a row whose
// publish fails transiently. Several relays share the rows, and each row is
// claimed by one relay at a time, but they publish in no particular order.
type Relay struct {
	eng     Engine
	db      sqlate.Beginner
	poll    time.Duration
	timeout time.Duration
	drain   time.Duration
	report  func(context.Context, error)
	ready   atomic.Bool
}

var _ reactor.Source[event.Event] = (*Relay)(nil)

// QuarantineError is the error the relay reports for a row it quarantined:
// one whose header or event does not decode, or whose publish failed with
// an error marked [event.Permanent]. The row's quarantine is committed by
// the time it is reported.
type QuarantineError struct {
	// Seq is the row's position in the outbox.
	Seq int64
	// Event is the row's event, or the zero Event when the row does not
	// decode.
	Event event.Event
	// Reason is why the row was quarantined, as the engine recorded it.
	Reason string
	// Err is the failure that quarantined the row: the decoding error, or
	// the handler's permanent error.
	Err error
}

func (e *QuarantineError) Error() string {
	return fmt.Sprintf("outbox: relay: row %d quarantined: %s", e.Seq, e.Reason)
}

func (e *QuarantineError) Unwrap() error { return e.Err }

// PublishError is the error the relay reports when a row's handler fails
// with an error not marked [event.Permanent]: the row stays unpublished, the
// pass ends, and the next pass retries the row.
type PublishError struct {
	// Seq is the row's position in the outbox.
	Seq int64
	// Event is the row's event.
	Event event.Event
	// Err is the handler's error.
	Err error
}

func (e *PublishError) Error() string {
	return fmt.Sprintf("outbox: relay: row %d: event %s: %v", e.Seq, e.Event.ID, e.Err)
}

func (e *PublishError) Unwrap() error { return e.Err }

// Receive publishes through fn until ctx ends, and returns nil: no row and
// no failure ends it. A transient handler error or a database error ends
// the pass, and both are reported; a database error also makes the relay
// not ready until a pass succeeds. A row whose handler is running when ctx
// ends is settled before Receive returns, and a relay with a [Drain] then
// makes its last pass.
func (r *Relay) Receive(ctx context.Context, fn reactor.Func[event.Event]) error {
	r.ready.Store(true)
	defer r.ready.Store(false)
	for ctx.Err() == nil {
		r.ready.Store(r.settle(ctx, r.pass(ctx, fn)))
		t := time.NewTimer(r.poll)
		select {
		case <-ctx.Done():
		case <-t.C:
		}
		t.Stop()
	}
	if r.drain > 0 {
		// The last pass's failure is reported and leaves its row for the
		// next start, as any failed pass does, so it is not Receive's error.
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.drain)
		defer cancel()
		r.settle(dctx, r.pass(dctx, fn))
	}
	return nil
}

// settle reports err, the outcome of a pass on ctx, unless it is ctx's own
// end, and returns whether the relay is ready after it: a pass that reached
// the database throughout, including one a handler's error ended, leaves it
// ready.
func (r *Relay) settle(ctx context.Context, err error) bool {
	if err == nil {
		return true
	}
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		// The stop interrupted the pass; nothing failed.
		return true
	}
	r.report(ctx, err)
	_, published := errors.AsType[*PublishError](err)
	return published || ctx.Err() != nil
}

// pass handles rows until none is left, returning nil, or until one fails.
func (r *Relay) pass(ctx context.Context, fn reactor.Func[event.Event]) error {
	for ctx.Err() == nil {
		handled, err := r.next(ctx, fn)
		if err != nil || !handled {
			return err
		}
	}
	return nil
}

// next handles the oldest unclaimed row, reporting whether there was one.
func (r *Relay) next(ctx context.Context, fn reactor.Func[event.Event]) (handled bool, err error) {
	// database/sql rolls a transaction back when the context it began with
	// ends, so the transaction begins on a context that outlives ctx: a row
	// claimed before ctx ends is settled, and its handler keeps only its own
	// deadline. Only the claim itself stops at ctx.
	// The transaction's own deadline, twice the handler's, bounds a stalled
	// database or a handler that ignores its deadline.
	settle, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*r.timeout)
	defer cancel()
	tx, err := r.db.Begin(settle)
	if err != nil {
		return false, fmt.Errorf("outbox: relay: begin: %w", err)
	}
	defer func() {
		if !handled || err != nil {
			_ = tx.Rollback()
		}
	}()

	claimed, err := r.eng.ClaimRow.Scan(scanRow).One(ctx, tx, nil)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("outbox: relay: claim: %w", err)
	}
	seq := claimed.seq

	var h event.Header
	if err := json.Unmarshal(claimed.header, &h); err != nil {
		return r.quarantine(ctx, settle, tx, &QuarantineError{
			Seq: seq, Reason: "header does not decode: " + err.Error(), Err: err,
		})
	}
	e, err := event.Decode(h, claimed.data)
	if err != nil {
		return r.quarantine(ctx, settle, tx, &QuarantineError{
			Seq: seq, Reason: "event does not decode: " + err.Error(), Err: err,
		})
	}

	hctx, cancelHandler := context.WithTimeout(settle, r.timeout)
	err = fn(hctx, e)
	cancelHandler()
	if event.IsPermanent(err) {
		return r.quarantine(ctx, settle, tx, &QuarantineError{
			Seq: seq, Event: e, Reason: "publish failed permanently: " + err.Error(), Err: err,
		})
	}
	if err != nil {
		return false, &PublishError{Seq: seq, Event: e, Err: err}
	}
	n, err := r.eng.MarkPublished.Exec(settle, tx, query.Args{"seq": seq})
	if err != nil {
		return false, fmt.Errorf("outbox: relay: marking row %d published: %w", seq, err)
	}
	if n != 1 {
		// An engine whose mark changes no row would have the relay publish
		// the row again on every pass.
		return false, fmt.Errorf("outbox: relay: marking row %d published changed %d rows, want 1", seq, n)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("outbox: relay: committing row %d: %w", seq, err)
	}
	return true, nil
}

// quarantine marks q's row quarantined in tx, the transaction that claimed
// it, commits, and reports q, so the pass goes on to the next row. A failure
// to quarantine is a database error, which ends the pass with the row left
// as it was.
func (r *Relay) quarantine(ctx, settle context.Context, tx *sqlate.Tx, q *QuarantineError) (bool, error) {
	n, err := r.eng.Quarantine.Exec(settle, tx, query.Args{"seq": q.Seq, "reason": q.Reason})
	if err != nil {
		return false, fmt.Errorf("outbox: relay: quarantining row %d: %w", q.Seq, err)
	}
	if n != 1 {
		// An engine whose quarantine changes no row would have the relay
		// claim the row again on every pass.
		return false, fmt.Errorf("outbox: relay: quarantining row %d changed %d rows, want 1", q.Seq, n)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("outbox: relay: committing row %d's quarantine: %w", q.Seq, err)
	}
	r.report(ctx, q)
	return true, nil
}

// Ready reports whether the relay is receiving and no pass since its last
// successful one has failed to reach the database. It is true from the
// start of Receive, before the first pass. A handler's error, such as a
// broker that is down, leaves the relay ready: the row waits for the next
// pass, and readiness speaks only for the database.
func (r *Relay) Ready() bool { return r.ready.Load() }

// row is one claimed outbox row.
type row struct {
	seq    int64
	header []byte
	data   []byte
}

// scanRow reads ClaimRow's columns in the order [Engine] fixes.
func scanRow(r query.Row) (row, error) {
	var v row
	err := r.Scan(&v.seq, &v.header, &v.data)
	return v, err
}
