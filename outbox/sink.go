package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/sqlate"
	"github.com/standards-lab/sqlate/query"
)

// sink writes each event as an outbox row in the command's transaction. The
// rows become visible to the relay when that transaction commits, and are
// discarded with it on a rollback.
type sink struct{ eng Engine }

// Write encodes each event and inserts it on tx, in order. An event whose
// source and id are already in the outbox fails, and so fails the
// transaction.
func (s sink) Write(ctx context.Context, tx *sqlate.Tx, es ...event.Event) error {
	for _, e := range es {
		h, data, err := event.Encode(e)
		if err != nil {
			return fmt.Errorf("outbox: write: %w", err)
		}
		header, err := json.Marshal(h)
		if err != nil {
			return fmt.Errorf("outbox: write %s: %w", e.ID, err)
		}
		args := query.Args{"source": e.Source, "id": e.ID, "header": header, "data": data}
		if _, err := s.eng.Emit.Exec(ctx, tx, args); err != nil {
			return fmt.Errorf("outbox: write %s: %w", e.ID, err)
		}
	}
	return nil
}
