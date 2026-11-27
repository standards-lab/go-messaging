package messaging

import (
	"context"
	"errors"
	"iter"

	"github.com/standards-lab/go-core/event"
)

// ErrConflict is the error [Journal.Append] wraps when the subject's last
// event is not the one the caller expected.
var ErrConflict = errors.New("messaging: the subject's last sequence is not the expected one")

// Entry is an event read from a journal subject, with its stream sequence.
type Entry struct {
	Event    event.Event
	Sequence uint64
}

// Journal appends events to subjects the caller names, each append guarded
// by the subject's last stream sequence, and reads a subject back in order.
// It sits beside [Broker], on the same stream: a provider implements both,
// and an appended event is a message on that stream like a published one.
type Journal interface {
	// Append stores e, which the caller has already stamped, on subject,
	// and returns its stream sequence. It succeeds only when the stream
	// sequence of subject's last event is expect, where 0 means subject has
	// no event yet; otherwise it fails with an error wrapping
	// [ErrConflict], naming subject and expect. On that mismatch, when
	// subject's last event has e's source and id, Append instead returns
	// that event's sequence, so a retry whose first append was stored
	// succeeds.
	//
	// Append checks expect before it deduplicates, with [Publisher.Publish],
	// in one window: when expect matches, a source and id the broker has
	// seen within its deduplication window is not stored again, and Append
	// returns the original event's sequence. A repeat whose expect is stale
	// fails with [ErrConflict] unless the original is subject's last event,
	// except on a provider that deduplicates first, such as a replicated
	// JetStream stream, where it returns the original event's sequence.
	//
	// Append rejects an event that [Encode] rejects, a header value holding
	// CR or LF, and a subject that breaks [CheckType].
	Append(ctx context.Context, subject string, expect uint64, e event.Event) (uint64, error)
	// Read returns subject's events after the stream sequence after, 0 for
	// all, in sequence order, ending at subject's last event as it stood
	// when the read began: an event appended during the read is not
	// yielded, and a subject with no events yields nothing. A message that
	// does not decode yields its error and ends the read. Stopping the
	// iteration early releases whatever the read holds.
	Read(ctx context.Context, subject string, after uint64) iter.Seq2[Entry, error]
}
