//go:build integration

package postgres_test

import (
	"slices"
	"testing"
	"time"

	"github.com/standards-lab/go-messaging/postgres"
	"github.com/standards-lab/sqlate"
)

func set(t *testing.T, db *sqlate.DB, statement string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(t.Context(), statement, args...); err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
}

func remaining(t *testing.T, db *sqlate.DB, table string) []string {
	t.Helper()
	r, err := db.QueryContext(t.Context(), `SELECT id FROM `+table+` ORDER BY id`)
	if err != nil {
		t.Fatalf("read %s: %v", table, err)
	}
	defer func() { _ = r.Close() }()
	var out []string
	for r.Next() {
		var id string
		if err := r.Scan(&id); err != nil {
			t.Fatalf("scan %s: %v", table, err)
		}
		out = append(out, id)
	}
	if err := r.Err(); err != nil {
		t.Fatalf("read %s: %v", table, err)
	}
	return out
}

func pass(t *testing.T, db *sqlate.DB, r *postgres.Retention, table string, budget int, wantMore bool, wantLeft ...string) {
	t.Helper()
	more, err := r.Pass(t.Context(), budget)
	if err != nil {
		t.Fatalf("pass with budget %d: %v", budget, err)
	}
	if more != wantMore {
		t.Errorf("pass with budget %d: more = %v, want %v", budget, more, wantMore)
	}
	if left := remaining(t, db, table); !slices.Equal(left, wantLeft) {
		t.Fatalf("after a pass with budget %d, %s holds %v, want %v", budget, table, left, wantLeft)
	}
}

func TestOutboxRetentionDeletesAgedPublishedRowsOldestFirst(t *testing.T) {
	db := migrated(t)
	emit(t, db, tick("1"), tick("2"), tick("3"), tick("4"), tick("5"), tick("6"))
	for id, ago := range map[string]string{"1": "3 hours", "2": "5 hours", "3": "4 hours", "4": "10 minutes"} {
		set(t, db, `UPDATE messaging_outbox SET published_at = now() - $1::interval WHERE id = $2`, ago, id)
	}
	set(t, db, `UPDATE messaging_outbox SET published_at = now() - interval '9 hours',
		quarantined_at = now(), quarantine_reason = 'test' WHERE id = '6'`)
	r, err := postgres.OutboxRetention(db, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	pass(t, db, r, "messaging_outbox", 2, true, "1", "4", "5", "6")
	pass(t, db, r, "messaging_outbox", 2, false, "4", "5", "6")
	pass(t, db, r, "messaging_outbox", 2, false, "4", "5", "6")
}

func TestOutboxRetentionFullBudgetReportsMore(t *testing.T) {
	db := migrated(t)
	emit(t, db, tick("1"), tick("2"))
	set(t, db, `UPDATE messaging_outbox SET published_at = now() - interval '2 hours'`)
	r, err := postgres.OutboxRetention(db, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	pass(t, db, r, "messaging_outbox", 2, true)
	pass(t, db, r, "messaging_outbox", 2, false)
}

func TestInboxRetentionDeletesAgedHandledRowsOldestFirst(t *testing.T) {
	db := migrated(t)
	for _, id := range []string{"1", "2", "3", "4"} {
		claim(t, db, "billing", tick(id), false)
	}
	claim(t, db, "audit", tick("5"), false)
	for id, ago := range map[string]string{"1": "3 hours", "2": "5 hours", "5": "4 hours", "3": "10 minutes"} {
		set(t, db, `UPDATE messaging_inbox SET handled_at = now() - $1::interval WHERE id = $2`, ago, id)
	}
	r, err := postgres.InboxRetention(db, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	pass(t, db, r, "messaging_inbox", 2, true, "1", "3", "4")
	pass(t, db, r, "messaging_inbox", 2, false, "3", "4")
	pass(t, db, r, "messaging_inbox", 2, false, "3", "4")
	if !claim(t, db, "billing", tick("2"), false) {
		t.Fatal("an event whose row was deleted was not first")
	}
}

func TestVerifyPreparesTheRetentions(t *testing.T) {
	db := migrated(t)
	set(t, db, `ALTER TABLE messaging_inbox RENAME COLUMN handled_at TO handled`)
	if err := postgres.Verify(t.Context(), db); err == nil {
		t.Fatal("Verify passed on an inbox without handled_at")
	}
}
