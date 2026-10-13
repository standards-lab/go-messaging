package inbox_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-messaging/inbox"
	"github.com/standards-lab/sqlate"
	"github.com/standards-lab/sqlate/query"
	"github.com/standards-lab/sqlate/sqltest"
)

// claim compiles a claim statement from text over sqlate's test dialect, so
// the engine contract is tested without a driver.
func claim(t *testing.T, text string) query.Statement {
	t.Helper()
	fsys := fstest.MapFS{"s/claim.sql": &fstest.MapFile{Data: []byte("--| tier: standard\n" + text)}}
	return query.MustCatalog(query.Patterns()).MustCompile(fsys, "s", sqltest.Dialect{}).Statement("claim")
}

const claimText = "INSERT INTO i (c, s, id) VALUES ({{consumer}}, {{source}}, {{id}})"

// newInbox returns an inbox over a complete engine, and a session over
// sqlate's scripted driver queued with responses.
func newInbox(t *testing.T, responses ...sqltest.Response) (*inbox.Inbox, *sqlate.DB, *sqltest.Recorder) {
	t.Helper()
	in, err := inbox.New(inbox.Engine{Claim: claim(t, "--| transaction: required\n"+claimText)})
	if err != nil {
		t.Fatal(err)
	}
	pool, rec := sqltest.Open(t, responses...)
	return in, sqlate.Wrap(pool, sqltest.Dialect{}), rec
}

var placed = event.Event{ID: "e1", Source: "test/orders", Type: "order.placed"}

// claimIn runs Claim in a transaction of its own, which commits unless
// rollback is set.
func claimIn(t *testing.T, in *inbox.Inbox, db *sqlate.DB, consumer string, rollback bool) (bool, error) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first, err := in.Claim(ctx, tx, consumer, placed)
	if rollback || err != nil {
		_ = tx.Rollback()
		return first, err
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return first, nil
}

func TestNewAcceptsACompleteEngine(t *testing.T) {
	if _, err := inbox.New(inbox.Engine{Claim: claim(t, claimText)}); err != nil {
		t.Fatal(err)
	}
}

func TestNewRequiresTheStatement(t *testing.T) {
	_, err := inbox.New(inbox.Engine{})
	if err == nil || !strings.Contains(err.Error(), "Claim is not defined") {
		t.Fatalf("New = %v, want the undefined Claim named", err)
	}
}

func TestNewChecksTheStatementsParameters(t *testing.T) {
	st := claim(t, "INSERT INTO i (c, id) VALUES ({{consumer}}, {{id}})")
	_, err := inbox.New(inbox.Engine{Claim: st})
	if err == nil || !strings.Contains(err.Error(), "Claim (claim) takes parameters") {
		t.Fatalf("New = %v, want Claim's parameters refused", err)
	}
}

func TestClaimIsFirstOnlyWhenItRecordsTheClaim(t *testing.T) {
	// The engine records the claim the first time, and changes nothing on a
	// repeat.
	in, db, rec := newInbox(t, sqltest.Response{Affected: 1}, sqltest.Response{Affected: 0})
	if first, err := claimIn(t, in, db, "orders", false); err != nil || !first {
		t.Fatalf("first Claim = %v, %v; want true", first, err)
	}
	if first, err := claimIn(t, in, db, "orders", false); err != nil || first {
		t.Fatalf("repeated Claim = %v, %v; want false", first, err)
	}
	for _, c := range rec.Calls() {
		if c.Op == sqltest.OpExec && !slices.Equal(c.Args, []any{"orders", "test/orders", "e1"}) {
			t.Errorf("claim bound %v, want the consumer, source, and id", c.Args)
		}
	}
}

func TestClaimRunsInTheHandlersTransaction(t *testing.T) {
	in, db, rec := newInbox(t, sqltest.Response{Affected: 1})
	if _, err := claimIn(t, in, db, "orders", true); err != nil {
		t.Fatal(err)
	}
	// The claim rolls back with the handler's work, so the engine releases
	// it for the redelivery.
	want := []sqltest.Op{sqltest.OpBegin, sqltest.OpExec, sqltest.OpRollback}
	if ops := rec.Ops(); !slices.Equal(ops, want) {
		t.Errorf("ops = %v, want %v", ops, want)
	}
}

func TestClaimRequiresAConsumer(t *testing.T) {
	in, db, rec := newInbox(t)
	if _, err := claimIn(t, in, db, "", false); err == nil {
		t.Fatal("Claim accepted an empty consumer")
	}
	if sql := rec.SQL(sqltest.OpExec); len(sql) != 0 {
		t.Errorf("Claim ran %v without a consumer", sql)
	}
}

func TestClaimFailsWithTheEngine(t *testing.T) {
	broken := errors.New("connection reset")
	in, db, _ := newInbox(t, sqltest.Response{Err: broken})
	if _, err := claimIn(t, in, db, "orders", false); !errors.Is(err, broken) {
		t.Fatalf("Claim = %v, want the engine's error", err)
	}
}
