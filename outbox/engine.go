package outbox

import (
	"errors"
	"fmt"

	"github.com/standards-lab/go-messaging/internal/engine"
	"github.com/standards-lab/sqlate/query"
)

// Engine is the SQL a database engine's module supplies: every statement the
// outbox runs, compiled for that engine. The outbox holds no SQL of its own,
// so an engine is required, and it must define each statement with exactly
// the parameters named here. [New] checks both. Emit runs on the command's
// transaction, a sqlate *Tx, so it may declare a transaction required.
type Engine struct {
	// Emit inserts one event into the outbox. Parameters: source, id,
	// header (event.Encode's headers as JSON), and data. An event whose
	// source and id are already in the outbox must fail.
	Emit query.Statement
	// ClaimRow locks the oldest row neither published nor quarantined that
	// is not already held by another transaction, skipping held rows, for
	// the rest of the transaction. It takes no parameters and returns seq,
	// header, and data, in that order, or no row. A statement exposes no
	// result columns, so [New] cannot check them; a mismatch fails the
	// relay's first claim.
	ClaimRow query.Statement
	// MarkPublished marks the claimed row published. Parameter: seq.
	MarkPublished query.Statement
	// Quarantine marks the claimed row quarantined, recording why, so
	// ClaimRow never returns it again. Parameters: seq and reason, a
	// human-readable account of why the row cannot be published.
	Quarantine query.Statement
}

// validate reports every statement the engine leaves undefined or defines
// with the wrong parameters.
func (e Engine) validate() error {
	err := errors.Join(
		engine.Check("Emit", e.Emit, "source", "id", "header", "data"),
		engine.Check("ClaimRow", e.ClaimRow),
		engine.Check("MarkPublished", e.MarkPublished, "seq"),
		engine.Check("Quarantine", e.Quarantine, "seq", "reason"),
	)
	if err != nil {
		return fmt.Errorf("outbox: engine: %w", err)
	}
	return nil
}
