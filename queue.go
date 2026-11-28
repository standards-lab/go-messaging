package messaging

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-core/reactor"
)

// Queue holds durable jobs: each event it takes waits until a worker
// acknowledges it or the queue dead-letters it, and then leaves the queue.
// It names no broker, so a provider meets it on whatever store holds the
// jobs.
type Queue interface {
	// Enqueue stores e and returns once the queue holds it. It rejects an
	// event that [Encode] rejects, as [Publisher.Publish] does, and
	// deduplicates the same way: an event whose source and id the queue has
	// seen within its deduplication window is accepted and stored once.
	Enqueue(ctx context.Context, e event.Event) error
	// Subscribe validates w and returns a source of its events: a member of
	// the worker group w.Name names, taking the events on w.Subject. An
	// unusable w fails Subscribe.
	//
	// Members of one Name share the queue: a member claims one event at a
	// time, and the next only once the handler's outcome is settled. While
	// the handler runs, the source extends the event's delivery, so a long
	// job keeps its claim; the handler is given no way to extend it. When
	// no extension has succeeded for w.AckWait, the handler's context ends.
	//
	// The outcome is the handler's return. nil acknowledges the event, which
	// leaves the queue. An error redelivers it after [Worker.RetryDelay] of
	// its attempt. An error marked by [event.Permanent], or any error on the
	// final attempt, dead-letters it. A final attempt that ends with no
	// outcome, because the worker stopped or lost its extension, dead-letters
	// the event on its next delivery, without calling the handler. A handler
	// error never ends the source.
	//
	// Each delivery's handler context carries its attempt, which [Attempt]
	// reads, from 1; [Worker.Final] reports whether it is the last. A source
	// on a dead-letter subject also puts each dead letter's [DeadLetter] on
	// the handler context, which [DeadLetterOf] reads.
	//
	// A dead letter is the original event, unchanged, stored on
	// w.DeadLetter with the headers [DeadLetter.Header] writes. It is stored
	// once however often dead-lettering repeats, it is never deduplicated
	// against the event's enqueue, and once it is stored the original is
	// gone. w.DeadLetter is a subject of the same queue, so a later worker
	// whose Subject it is takes the dead letters and acknowledges each off.
	//
	// When the source drains, a running handler's context ends, and its
	// event is settled or left for redelivery, never lost.
	//
	// The source passes report each error it survives, with the context it
	// was receiving under. A nil report discards them.
	Subscribe(w Worker, report Report) (reactor.Source[event.Event], error)
}

// Worker describes a durable group of workers on a [Queue].
type Worker struct {
	// Name is the worker group: sources subscribed under the same Name share
	// the queue's events. It must pass [CheckName].
	Name string
	// Subject is the subject the group takes events from: empty for the
	// events enqueued, or a dead-letter subject for the dead letters stored
	// there. A set Subject must pass [CheckType].
	Subject string
	// MaxDeliver bounds the attempts on one event; it must be positive. The
	// attempt numbered MaxDeliver is the final one.
	MaxDeliver int
	// AckWait is how long a delivery lasts with no successful extension
	// before the handler's context ends and the event is redelivered. 0 is
	// [DefaultAckWait].
	AckWait time.Duration
	// Backoff is how long an event waits before its redelivery after a
	// failed attempt, as [Worker.RetryDelay] reads it. No entry may be
	// negative.
	Backoff []time.Duration
	// DeadLetter is the subject the group's dead letters are stored on. It
	// is required, must pass [CheckType], and must differ from Subject.
	DeadLetter string
}

// Validate reports every way w is unusable.
func (w Worker) Validate() error {
	var errs []error
	if err := CheckName(w.Name); err != nil {
		errs = append(errs, fmt.Errorf("name: %w", err))
	}
	if w.Subject != "" {
		if err := CheckType(w.Subject); err != nil {
			errs = append(errs, fmt.Errorf("subject: %w", err))
		}
	}
	if w.MaxDeliver <= 0 {
		errs = append(errs, fmt.Errorf("max deliver must be positive, got %d", w.MaxDeliver))
	}
	if w.AckWait < 0 {
		errs = append(errs, errors.New("ack wait must not be negative"))
	}
	for i, d := range w.Backoff {
		if d < 0 {
			errs = append(errs, fmt.Errorf("backoff[%d] must not be negative, got %v", i, d))
		}
	}
	if w.DeadLetter == "" {
		errs = append(errs, errors.New("dead letter: a subject is required"))
	} else if err := CheckType(w.DeadLetter); err != nil {
		errs = append(errs, fmt.Errorf("dead letter: %w", err))
	} else if w.DeadLetter == w.Subject {
		errs = append(errs, fmt.Errorf("dead letter: %q is the worker's own subject", w.DeadLetter))
	}
	if len(errs) > 0 {
		return fmt.Errorf("messaging: worker: %w", errors.Join(errs...))
	}
	return nil
}

// Normalize returns w in the form a provider configures its worker group
// from: AckWait defaulted to [DefaultAckWait], and Backoff copied, nil when
// empty, so the caller cannot change it afterward.
func (w Worker) Normalize() Worker {
	if w.AckWait == 0 {
		w.AckWait = DefaultAckWait
	}
	if len(w.Backoff) == 0 {
		w.Backoff = nil
	} else {
		w.Backoff = append([]time.Duration(nil), w.Backoff...)
	}
	return w
}

// RetryDelay returns how long an event waits before its redelivery after
// failed attempt n: Backoff[n-1], the last entry for an attempt past the
// end of Backoff, and 0, a redelivery at once, when Backoff is empty. An
// attempt below 1 reads as the first.
func (w Worker) RetryDelay(attempt int) time.Duration {
	if len(w.Backoff) == 0 {
		return 0
	}
	i := min(max(attempt, 1), len(w.Backoff)) - 1
	return w.Backoff[i]
}

// Final reports whether the delivery whose handler context is ctx is the
// event's final attempt: its [Attempt] has reached MaxDeliver. A context
// no provider gave has no attempt, and is not final.
func (w Worker) Final(ctx context.Context) bool {
	n := Attempt(ctx)
	return n > 0 && n >= w.MaxDeliver
}

const (
	// HeaderAttempts is the dead-letter header holding how many attempts
	// were made on the event, in decimal.
	HeaderAttempts = "messaging-attempts"
	// HeaderOutcome is the dead-letter header holding how the last attempt
	// ended: [OutcomeError] or [OutcomeNoError].
	HeaderOutcome = "messaging-outcome"
	// HeaderLastError is the dead-letter header holding the last attempt's
	// error, present only when the outcome is [OutcomeError].
	HeaderLastError = "messaging-last-error"
)

const (
	// OutcomeError is the [HeaderOutcome] of a dead letter whose last
	// attempt returned an error.
	OutcomeError = "error"
	// OutcomeNoError is the [HeaderOutcome] of a dead letter whose last
	// attempt returned no error: the worker stopped or lost its extension.
	OutcomeNoError = "no-error"
)

// DeadLetter is what a dead letter's headers say of the event's attempts.
type DeadLetter struct {
	// Attempts is how many attempts were made on the event.
	Attempts int
	// Errored reports whether the last attempt returned an error.
	Errored bool
	// Err is the last attempt's error text, when Errored.
	Err string
}

// Header returns d as the headers a provider writes on a dead letter, beside
// the event's own: [HeaderAttempts], [HeaderOutcome], and, when d.Errored,
// [HeaderLastError], with each CR or LF in the error written as a space.
// The names are not prefixed with "ce-", so the event decodes from the dead
// letter unchanged.
func (d DeadLetter) Header() event.Header {
	h := event.Header{
		HeaderAttempts: {strconv.Itoa(d.Attempts)},
		HeaderOutcome:  {OutcomeNoError},
	}
	if d.Errored {
		h[HeaderOutcome] = []string{OutcomeError}
		h[HeaderLastError] = []string{strings.Map(func(r rune) rune {
			if r == '\r' || r == '\n' {
				return ' '
			}
			return r
		}, d.Err)}
	}
	return h
}

// ReadDeadLetter reads a dead letter's [DeadLetter] from its headers, the
// inverse of [DeadLetter.Header]. Header names match case-insensitively, as
// [event.Decode] matches them. It fails on a missing or malformed attempt
// count or outcome.
func ReadDeadLetter(h event.Header) (DeadLetter, error) {
	var d DeadLetter
	attempts, ok := header(h, HeaderAttempts)
	if !ok {
		return DeadLetter{}, fmt.Errorf("messaging: dead letter: %s is missing", HeaderAttempts)
	}
	n, err := strconv.Atoi(attempts)
	if err != nil || n < 0 {
		return DeadLetter{}, fmt.Errorf("messaging: dead letter: %s %q is not an attempt count", HeaderAttempts, attempts)
	}
	d.Attempts = n
	outcome, _ := header(h, HeaderOutcome)
	switch outcome {
	case OutcomeError:
		d.Errored = true
		d.Err, _ = header(h, HeaderLastError)
	case OutcomeNoError:
	default:
		return DeadLetter{}, fmt.Errorf("messaging: dead letter: %s %q is not an outcome", HeaderOutcome, outcome)
	}
	return d, nil
}

func header(h event.Header, name string) (string, bool) {
	if vs := h[name]; len(vs) > 0 {
		return vs[0], true
	}
	for key, vs := range h {
		if len(vs) > 0 && strings.EqualFold(key, name) {
			return vs[0], true
		}
	}
	return "", false
}

type deadLetterKey struct{}

// WithDeadLetter returns ctx carrying d, the [DeadLetter] of the dead
// letter its handler is given. A provider calls it on each delivery's
// handler context on a dead-letter subject.
func WithDeadLetter(ctx context.Context, d DeadLetter) context.Context {
	return context.WithValue(ctx, deadLetterKey{}, d)
}

// DeadLetterOf returns the [DeadLetter] of the dead letter whose handler
// context is ctx, and false for a context no provider gave one.
func DeadLetterOf(ctx context.Context) (DeadLetter, bool) {
	d, ok := ctx.Value(deadLetterKey{}).(DeadLetter)
	return d, ok
}
