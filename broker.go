package messaging

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-core/reactor"
)

const (
	// DefaultAckWait is the AckWait of a subscription that sets none.
	DefaultAckWait = 30 * time.Second
	// DefaultDuplicates is a broker's deduplication window when it sets
	// none: JetStream's own default.
	DefaultDuplicates = 2 * time.Minute
)

// Publisher publishes an event. Publish returns once the broker holds the
// event, so a delivery can outlive the publisher.
//
// Publish rejects an event that [Encode] rejects. The broker deduplicates on
// the event's source and id, which together identify a CloudEvents event: an
// event whose source and id the broker has seen within its deduplication
// window is accepted and dropped, so a publisher that retries, such as the
// outbox's relay, delivers the event once, while another source's event that
// reuses the id still delivers.
type Publisher interface {
	Publish(ctx context.Context, e event.Event) error
}

// Broker publishes events and subscribes reactors to them.
type Broker interface {
	Publisher
	// Subscribe validates sub and returns a source of its events: a member
	// of the durable consumer sub.Name names.
	//
	// The first subscription under a Name creates its consumer, which starts
	// where sub.Start says: under [StartAll], at the beginning of the stream,
	// so it receives every event, including those published before it
	// subscribed; under [StartNew], at its own creation. A later subscription
	// under the same Name binds that consumer and must match its
	// configuration; a mismatch fails Subscribe, or the source's Receive when
	// the provider binds there.
	//
	// A member claims one delivery at a time, and the next only once the
	// handler's outcome is settled, so members of one Name share the work
	// rather than one member taking a batch. The outcome is the handler's
	// return: nil acknowledges the delivery; an error redelivers it after
	// sub.RetryDelay, until sub.MaxDeliver deliveries have been made; an
	// error marked by [event.Permanent] terminates it, so it is never
	// delivered again. A handler error never ends the source.
	//
	// Each delivery's handler context carries its attempt, which [Attempt]
	// reads: 1 on the first delivery of an event, and one more on each
	// redelivery, whether the previous delivery's handler returned an error
	// or overran AckWait.
	//
	// The source passes report each error it survives, one that does not
	// end its Receive, such as a message it cannot decode, with the context
	// it was receiving under. A nil report discards them.
	Subscribe(sub Subscription, report Report) (reactor.Source[event.Event], error)
}

// Report receives an error a source survives: one it handled itself and
// kept receiving after, which would otherwise go unseen. It must not block
// for long, since the source waits on it before its next delivery.
type Report func(ctx context.Context, err error)

// attemptKey is the context key under which a provider puts a delivery's
// attempt on its handler's context.
type attemptKey struct{}

// WithAttempt returns ctx carrying attempt, the number of the delivery its
// handler is given: 1 for the first. A provider calls it on each delivery's
// handler context.
func WithAttempt(ctx context.Context, attempt int) context.Context {
	return context.WithValue(ctx, attemptKey{}, attempt)
}

// Attempt returns the attempt of the delivery whose handler context is ctx:
// 1 on the first delivery of an event, and one more on each redelivery, so
// a handler on attempt MaxDeliver is the event's last. A context no provider
// gave has no attempt, and Attempt returns 0.
func Attempt(ctx context.Context) int {
	n, _ := ctx.Value(attemptKey{}).(int)
	return n
}

// Start is where a new durable consumer begins in the stream.
type Start int

const (
	// StartAll begins at the stream's beginning, so the consumer receives
	// every event the stream retains, including those published before it
	// was created.
	StartAll Start = iota
	// StartNew begins at the consumer's creation, so the consumer receives
	// only the events published after it. A provider may create the consumer
	// as late as the source's first Receive, so an event published between
	// Subscribe and that Receive may be skipped; a caller that must not miss
	// one uses StartAll.
	StartNew
)

// Subscription describes a durable consumer of events.
type Subscription struct {
	// Name is the durable consumer, and so its delivery group: sources
	// subscribed under the same Name share one position and split the work.
	// It is a token: no whitespace, '.', '*', '>', '/', or '\'.
	Name string
	// Types filters on the event's type; empty matches every type. Each
	// entry must pass [CheckType].
	Types []string
	// MaxDeliver bounds the deliveries of one event; 0 is unlimited.
	MaxDeliver int
	// AckWait is the handler's deadline for each delivery, and so the
	// deadline on its context. When it passes, the event is redelivered and
	// the late outcome is ignored. 0 is [DefaultAckWait].
	AckWait time.Duration
	// RetryDelay is how long an event whose handler returned an error waits
	// before it is redelivered; 0 redelivers it at once.
	RetryDelay time.Duration
	// Start is where the Name's consumer begins when this subscription
	// creates it; the zero value is [StartAll]. It is part of the consumer's
	// configuration, so a later subscription under the Name must match it,
	// even though it moves nothing once the consumer exists.
	Start Start
}

// Validate reports every way sub is unusable.
func (sub Subscription) Validate() error {
	var errs []error
	if err := CheckName(sub.Name); err != nil {
		errs = append(errs, fmt.Errorf("name: %w", err))
	}
	for _, t := range sub.Types {
		if err := CheckType(t); err != nil {
			errs = append(errs, fmt.Errorf("types: %w", err))
		}
	}
	if sub.MaxDeliver < 0 {
		errs = append(errs, errors.New("max deliver must not be negative"))
	}
	if sub.AckWait < 0 {
		errs = append(errs, errors.New("ack wait must not be negative"))
	}
	if sub.RetryDelay < 0 {
		errs = append(errs, errors.New("retry delay must not be negative"))
	}
	if sub.Start != StartAll && sub.Start != StartNew {
		errs = append(errs, fmt.Errorf("start %d is not a start position", sub.Start))
	}
	if len(errs) > 0 {
		return fmt.Errorf("messaging: subscription: %w", errors.Join(errs...))
	}
	return nil
}

// Normalize returns sub in the form a provider configures its consumer from:
// AckWait defaulted to [DefaultAckWait], and Types copied, sorted, and
// without repeats, nil when empty. Subscriptions that mean the same thing
// normalize equal, so they bind the same consumer, and the caller cannot
// change the filter afterward.
func (sub Subscription) Normalize() Subscription {
	if sub.AckWait == 0 {
		sub.AckWait = DefaultAckWait
	}
	if len(sub.Types) == 0 {
		sub.Types = nil
	} else {
		sub.Types = slices.Compact(slices.Sorted(slices.Values(sub.Types)))
	}
	return sub
}

// CheckName reports whether s can name a durable consumer or a stream: it is
// non-empty, with no whitespace, control character, '.', '*', '>', '/', or
// '\', the characters a JetStream name may not hold.
func CheckName(s string) error {
	if s == "" {
		return errors.New("a name is required")
	}
	if strings.ContainsFunc(s, func(r rune) bool {
		return strings.ContainsRune(".*>/\\", r) || unicode.IsSpace(r) || unicode.IsControl(r)
	}) {
		return fmt.Errorf("%q must not contain whitespace, a control character, '.', '*', '>', '/', or '\\'", s)
	}
	return nil
}

// CheckType reports whether t is an event type a broker can route: one or
// more tokens separated by '.', none of them empty, with no whitespace,
// control character, '*', or '>'. It is [event.CheckType], the rule a
// domain's event kinds are declared under, so a type a domain can define is
// a type a broker routes.
func CheckType(t string) error { return event.CheckType(t) }

// Encode is the check and encoding every provider's Publish makes: e must
// pass [event.Event.Validate] and its type [CheckType], and it is returned
// in binary content mode, as [event.Encode] gives it.
func Encode(e event.Event) (event.Header, []byte, error) {
	h, body, err := event.Encode(e)
	if err != nil {
		return nil, nil, err
	}
	if err := CheckType(e.Type); err != nil {
		return nil, nil, err
	}
	return h, body, nil
}

// Matches reports whether sub's type filter admits an event of type t.
func (sub Subscription) Matches(t string) bool {
	if len(sub.Types) == 0 {
		return true
	}
	return slices.Contains(sub.Types, t)
}
