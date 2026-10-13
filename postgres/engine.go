package postgres

import (
	"context"
	"embed"

	"github.com/standards-lab/go-messaging/inbox"
	"github.com/standards-lab/go-messaging/outbox"
	"github.com/standards-lab/sqlate"
	pgdialect "github.com/standards-lab/sqlate/postgres"
	"github.com/standards-lab/sqlate/query"
)

//go:embed statements/*.sql
var statementFiles embed.FS

// statements is the outbox's and the inbox's SQL, compiled once for
// Postgres. The files include no patterns, so the catalog is sqlate's own.
// A compile error is a defect in the embedded files, which any test of the
// package finds.
var statements = query.MustCatalog(query.Patterns()).MustCompile(statementFiles, "statements", pgdialect.Dialect{})

// Outbox returns the outbox's statements for Postgres, for [outbox.New].
func Outbox() outbox.Engine {
	return outbox.Engine{
		Emit:          statements.Statement("emit"),
		ClaimRow:      statements.Statement("claim_row"),
		MarkPublished: statements.Statement("mark_published"),
		Quarantine:    statements.Statement("quarantine_row"),
	}
}

// Inbox returns the inbox's statement for Postgres, for [inbox.New].
func Inbox() inbox.Engine {
	return inbox.Engine{Claim: statements.Statement("claim_inbox")}
}

// countPending is outside the Engine contract: the outbox itself never
// counts its rows.
var countPending = statements.Statement("count_pending").Scan(query.Scalar[int])

// Pending counts the outbox rows awaiting publication, neither published nor
// quarantined, for a consumer that reports or monitors the outbox's lag.
func Pending(ctx context.Context, sess sqlate.Session) (int, error) {
	return countPending.One(ctx, sess, nil)
}

// Statements returns the compiled statements in name order, for a consumer
// that lists the SQL its program runs.
func Statements() []query.Statement { return statements.Statements() }

// Verify prepares the statements against the schema sess reaches, so a
// schema without the messaging set, or a set behind these statements, fails
// at start rather than at the first write or claim. A consumer calls it from
// its verify stage, after the migrations.
func Verify(ctx context.Context, sess sqlate.Session) error { return statements.Verify(ctx, sess) }
