package outbox_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-messaging/outbox"
	"github.com/standards-lab/sqlate"
	"github.com/standards-lab/sqlate/query"
	"github.com/standards-lab/sqlate/sqltest"
)

// compile builds statements from text keyed by name, over sqlate's test
// dialect, so the engine contract is tested without a driver.
func compile(t *testing.T, files map[string]string) *query.Statements {
	t.Helper()
	fsys := fstest.MapFS{}
	for name, text := range files {
		fsys["s/"+name+".sql"] = &fstest.MapFile{Data: []byte("--| tier: standard\n" + text)}
	}
	return query.MustCatalog(query.Patterns()).MustCompile(fsys, "s", sqltest.Dialect{})
}

const (
	emit       = "INSERT INTO o (s, id, h, d) VALUES ({{source}}, {{id}}, {{header}}, {{data}})"
	quarantine = "UPDATE o SET q = now(), r = {{reason}} WHERE seq = {{seq}}"
)

// engineWith returns a complete engine whose Emit and Quarantine are the
// texts given.
func engineWith(t *testing.T, emit, quarantine string) outbox.Engine {
	t.Helper()
	s := compile(t, map[string]string{
		"emit":           emit,
		"claim_row":      "SELECT seq, header, data FROM o",
		"mark_published": "UPDATE o SET p = 1 WHERE seq = {{seq}}",
		"quarantine":     quarantine,
	})
	return outbox.Engine{
		Emit:          s.Statement("emit"),
		ClaimRow:      s.Statement("claim_row"),
		MarkPublished: s.Statement("mark_published"),
		Quarantine:    s.Statement("quarantine"),
	}
}

// newOutbox returns an outbox over a complete engine.
func newOutbox(t *testing.T) *outbox.Outbox {
	t.Helper()
	ob, err := outbox.New(engineWith(t, emit, quarantine))
	if err != nil {
		t.Fatal(err)
	}
	return ob
}

// open returns a session over sqlate's scripted driver, queued with
// responses, and the recorder behind it.
func open(t *testing.T, responses ...sqltest.Response) (*sqlate.DB, *sqltest.Recorder) {
	t.Helper()
	pool, rec := sqltest.Open(t, responses...)
	return sqlate.Wrap(pool, sqltest.Dialect{}), rec
}

// testEvent returns a valid event with id.
func testEvent(id string) event.Event {
	return event.Event{
		ID:     id,
		Source: "test/orders",
		Type:   "order.placed",
		Time:   time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC),
		Data:   []byte(`{"order":"` + id + `"}`),
	}
}

func TestNewAcceptsACompleteEngine(t *testing.T) {
	if _, err := outbox.New(engineWith(t, emit, quarantine)); err != nil {
		t.Fatal(err)
	}
}

func TestNewRequiresEveryStatement(t *testing.T) {
	_, err := outbox.New(outbox.Engine{})
	if err == nil {
		t.Fatal("New accepted an engine with no statements")
	}
	for _, field := range []string{"Emit", "ClaimRow", "MarkPublished", "Quarantine"} {
		if !strings.Contains(err.Error(), field+" is not defined") {
			t.Errorf("error %q does not name the undefined %s", err, field)
		}
	}
}

func TestNewChecksEachStatementsParameters(t *testing.T) {
	missing := "INSERT INTO o (s, id, h) VALUES ({{source}}, {{id}}, {{header}})"
	_, err := outbox.New(engineWith(t, missing, quarantine))
	if err == nil || !strings.Contains(err.Error(), "Emit (emit) takes parameters") {
		t.Fatalf("New = %v, want Emit's parameters refused", err)
	}
}

func TestNewChecksQuarantinesParameters(t *testing.T) {
	noReason := "UPDATE o SET q = now() WHERE seq = {{seq}}"
	_, err := outbox.New(engineWith(t, emit, noReason))
	if err == nil || !strings.Contains(err.Error(), "Quarantine (quarantine) takes parameters") {
		t.Fatalf("New = %v, want Quarantine's parameters refused", err)
	}
}

func TestNewNamesEveryRefusedStatement(t *testing.T) {
	missing := "INSERT INTO o (s, id, h) VALUES ({{source}}, {{id}}, {{header}})"
	noReason := "UPDATE o SET q = now() WHERE seq = {{seq}}"
	_, err := outbox.New(engineWith(t, missing, noReason))
	if err == nil {
		t.Fatal("New accepted an engine with two refused statements")
	}
	for _, field := range []string{"Emit (emit)", "Quarantine (quarantine)"} {
		if !strings.Contains(err.Error(), field) {
			t.Errorf("error %q does not name %s", err, field)
		}
	}
}

func TestNewAcceptsARequiredTransactionOnEmit(t *testing.T) {
	if _, err := outbox.New(engineWith(t, "--| transaction: required\n"+emit, quarantine)); err != nil {
		t.Fatalf("New = %v; Emit runs on a sqlate *Tx, so it may require one", err)
	}
}

func TestSinkWritesEachEventInTheCommandsTransaction(t *testing.T) {
	db, rec := open(t, sqltest.Response{Affected: 1}, sqltest.Response{Affected: 1})
	sink := newOutbox(t).Sink()
	_, err := sqlate.Transact(context.Background(), db, func(tx *sqlate.Tx) (struct{}, error) {
		return struct{}{}, sink.Write(context.Background(), tx, testEvent("a"), testEvent("b"))
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []sqltest.Op{sqltest.OpBegin, sqltest.OpExec, sqltest.OpExec, sqltest.OpCommit}
	if ops := rec.Ops(); !slices.Equal(ops, want) {
		t.Fatalf("ops = %v, want %v: both events in the one transaction", ops, want)
	}
	var ids []any
	for _, c := range rec.Calls() {
		if c.Op == sqltest.OpExec {
			// Emit binds source, id, header, and data, in that order.
			if c.Args[0] != "test/orders" {
				t.Errorf("source = %v, want test/orders", c.Args[0])
			}
			ids = append(ids, c.Args[1])
		}
	}
	if want := []any{"a", "b"}; !slices.Equal(ids, want) {
		t.Errorf("ids written = %v, want %v, in raise order", ids, want)
	}
}

func TestSinkFailsOnAnEventAlreadyThere(t *testing.T) {
	duplicate := errors.New("duplicate key")
	db, rec := open(t, sqltest.Response{Affected: 1}, sqltest.Response{Err: duplicate})
	sink := newOutbox(t).Sink()
	_, err := sqlate.Transact(context.Background(), db, func(tx *sqlate.Tx) (struct{}, error) {
		return struct{}{}, sink.Write(context.Background(), tx, testEvent("a"), testEvent("b"), testEvent("c"))
	})
	if !errors.Is(err, duplicate) {
		t.Fatalf("Write = %v, want the engine's duplicate error", err)
	}
	want := []sqltest.Op{sqltest.OpBegin, sqltest.OpExec, sqltest.OpExec, sqltest.OpRollback}
	if ops := rec.Ops(); !slices.Equal(ops, want) {
		t.Errorf("ops = %v, want %v: the write stops and the transaction rolls back", ops, want)
	}
}

func TestSinkRejectsAnInvalidEvent(t *testing.T) {
	db, rec := open(t)
	sink := newOutbox(t).Sink()
	bad := testEvent("a")
	bad.Type = ""
	_, err := sqlate.Transact(context.Background(), db, func(tx *sqlate.Tx) (struct{}, error) {
		return struct{}{}, sink.Write(context.Background(), tx, bad)
	})
	if err == nil {
		t.Fatal("Write accepted an event without a type")
	}
	if sql := rec.SQL(sqltest.OpExec); len(sql) != 0 {
		t.Errorf("Write ran %v for an invalid event", sql)
	}
}
