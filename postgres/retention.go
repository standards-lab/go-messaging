package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/standards-lab/sqlate"
	"github.com/standards-lab/sqlate/query"
)

// The retention statements are outside the Engine contracts, as
// countPending is: neither the relay nor the inbox's claim deletes a row.
var (
	reclaimOutbox = statements.Statement("reclaim_outbox")
	reclaimInbox  = statements.Statement("reclaim_inbox")
)

// Retention deletes the rows its table no longer needs, a budget at a
// time: the published outbox rows, or the handled inbox rows, older than
// its age. [OutboxRetention] and [InboxRetention] build one. Its Pass is a
// reclaimer's, so a sweeper that bounds each pass by a budget drives it.
type Retention struct {
	sess    sqlate.Session
	age     time.Duration
	reclaim query.Statement
}

// OutboxRetention returns the outbox's retention over sess: each pass
// deletes the published rows published more than age ago, oldest first.
// A row awaiting publication or quarantined is never deleted. An age that
// is not positive is an error; the module has no default, since how long
// a published row is worth keeping is the consumer's call.
func OutboxRetention(sess sqlate.Session, age time.Duration) (*Retention, error) {
	return newRetention(sess, age, reclaimOutbox)
}

// InboxRetention returns the inbox's retention over sess: each pass
// deletes the rows handled more than age ago, oldest first. A redelivery
// of an event whose row was deleted is claimed again, so age must outlast
// every redelivery the broker can make. An age that is not positive is an
// error; the module has no default.
func InboxRetention(sess sqlate.Session, age time.Duration) (*Retention, error) {
	return newRetention(sess, age, reclaimInbox)
}

func newRetention(sess sqlate.Session, age time.Duration, reclaim query.Statement) (*Retention, error) {
	if age <= 0 {
		return nil, fmt.Errorf("go-messaging/postgres: the retention age %s is not positive", age)
	}
	return &Retention{sess: sess, age: age, reclaim: reclaim}, nil
}

// Pass deletes at most budget rows older than the retention's age, oldest
// first, in one statement on the retention's session, and reports more
// while rows that were eligible may remain. The age is measured from now
// on this process's clock. more is true exactly when the pass deleted its
// whole budget: a pass that deleted fewer found every eligible row, so
// none remained, and a pass that deleted budget rows may have left more.
// A table holding exactly budget eligible rows therefore costs one extra
// pass that deletes none and reports false; the rule needs no second
// query. A budget below 1 is an error.
func (r *Retention) Pass(ctx context.Context, budget int) (more bool, err error) {
	if budget < 1 {
		return false, fmt.Errorf("go-messaging/postgres: the retention budget %d is below 1", budget)
	}
	before := time.Now().Add(-r.age).UTC()
	n, err := r.reclaim.Exec(ctx, r.sess, query.Args{"before": before, "budget": budget})
	if err != nil {
		return false, fmt.Errorf("go-messaging/postgres: %s: %w", r.reclaim.Name(), err)
	}
	return n == int64(budget), nil
}
