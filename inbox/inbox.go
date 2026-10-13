package inbox

import (
	"context"
	"errors"
	"fmt"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-messaging/internal/engine"
	"github.com/standards-lab/sqlate"
	"github.com/standards-lab/sqlate/query"
)

// Engine is the SQL a database engine's module supplies for the inbox. The
// inbox holds no SQL of its own, so an engine is required, and [New] checks
// that it defines its statement with exactly the parameters named here.
type Engine struct {
	// Claim records a consumer's handling of an event, affecting one row on
	// the first claim and none on a repeat. Parameters: consumer, source, and
	// id. It runs on the handler's transaction, a sqlate *Tx, so it may
	// declare a transaction required.
	Claim query.Statement
}

// Inbox claims events for their consumers on one engine's statement.
type Inbox struct {
	eng Engine
}

// New returns an inbox over eng, once eng defines its statement with the
// parameters [Engine] names.
func New(eng Engine) (*Inbox, error) {
	if err := engine.Check("Claim", eng.Claim, "consumer", "source", "id"); err != nil {
		return nil, fmt.Errorf("inbox: engine: %w", err)
	}
	return &Inbox{eng: eng}, nil
}

// Claim records that consumer is handling e, in the handler's transaction
// tx, and reports whether this is the first time. A handler that gets false
// has already committed its handling of e, so it skips the event and
// acknowledges it. A handler that gets true does its work on the same tx; a
// rollback releases the claim, so a redelivery can claim e again.
//
// consumer names the handler, typically its subscription's Name: consumers
// claim an event independently. The claim keys on e's source and id, the
// pair that identifies a CloudEvents event.
func (in *Inbox) Claim(ctx context.Context, tx *sqlate.Tx, consumer string, e event.Event) (first bool, err error) {
	if consumer == "" {
		return false, errors.New("inbox: claim: consumer is required")
	}
	n, err := in.eng.Claim.Exec(ctx, tx, query.Args{"consumer": consumer, "source": e.Source, "id": e.ID})
	if err != nil {
		return false, fmt.Errorf("inbox: claim %s for %s: %w", e.ID, consumer, err)
	}
	return n == 1, nil
}
