package postgres_test

import (
	"testing"
	"time"

	"github.com/standards-lab/go-messaging/postgres"
)

var retentions = map[string]func(time.Duration) (*postgres.Retention, error){
	"outbox": func(age time.Duration) (*postgres.Retention, error) { return postgres.OutboxRetention(nil, age) },
	"inbox":  func(age time.Duration) (*postgres.Retention, error) { return postgres.InboxRetention(nil, age) },
}

func TestRetentionRefusesAnAgeThatIsNotPositive(t *testing.T) {
	for table, build := range retentions {
		for _, age := range []time.Duration{0, -time.Hour} {
			if r, err := build(age); err == nil {
				t.Errorf("%s retention with age %s = %v, want an error", table, age, r)
			}
		}
		if _, err := build(time.Nanosecond); err != nil {
			t.Errorf("%s retention with a positive age: %v", table, err)
		}
	}
}

func TestPassRefusesABudgetBelowOne(t *testing.T) {
	for table, build := range retentions {
		r, err := build(time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		for _, budget := range []int{0, -1} {
			if more, err := r.Pass(t.Context(), budget); err == nil {
				t.Errorf("%s pass with budget %d = %v, want an error", table, budget, more)
			}
		}
	}
}
