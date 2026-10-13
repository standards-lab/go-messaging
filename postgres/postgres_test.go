package postgres_test

import (
	"testing"

	"github.com/standards-lab/go-messaging/inbox"
	"github.com/standards-lab/go-messaging/outbox"
	"github.com/standards-lab/go-messaging/postgres"
)

// The engine defines every statement with the parameters the outbox and the
// inbox name.
func TestEnginesAreComplete(t *testing.T) {
	if _, err := outbox.New(postgres.Outbox()); err != nil {
		t.Fatal(err)
	}
	if _, err := inbox.New(postgres.Inbox()); err != nil {
		t.Fatal(err)
	}
}

func TestMigrationsName(t *testing.T) {
	set, err := postgres.Migrations()
	if err != nil {
		t.Fatal(err)
	}
	if set.Name != postgres.Source || set.Table != postgres.Table || len(set.Migrations) != 1 {
		t.Fatalf("set = %s/%s with %d migrations", set.Name, set.Table, len(set.Migrations))
	}
}

// Every statement that writes declares its transaction: the relay's claim,
// mark, and quarantine run on its own, and the sink's and the inbox's on the
// command's. Only the count runs on a pool.
func TestStatementsDeclareTheirTransaction(t *testing.T) {
	want := map[string]bool{
		"claim_inbox":    true,
		"count_pending":  false,
		"claim_row":      true,
		"emit":           true,
		"mark_published": true,
		"quarantine_row": true,
	}
	got := map[string]bool{}
	for _, st := range postgres.Statements() {
		got[st.Name()] = st.TransactionRequired()
	}
	if len(got) != len(want) {
		t.Fatalf("statements %v, want %v", got, want)
	}
	for name, required := range want {
		if r, ok := got[name]; !ok || r != required {
			t.Errorf("%s: transaction required = %v (present %v), want %v", name, r, ok, required)
		}
	}
}
