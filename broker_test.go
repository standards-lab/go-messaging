package messaging_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/standards-lab/go-core/event"
	"github.com/standards-lab/go-messaging"
)

func TestValidate(t *testing.T) {
	if err := (messaging.Subscription{Name: "workers", Types: []string{"a"}}).Validate(); err != nil {
		t.Errorf("valid subscription: %v", err)
	}
	if err := (messaging.Subscription{Name: "workers", Start: messaging.StartNew}).Validate(); err != nil {
		t.Errorf("valid StartNew subscription: %v", err)
	}
	cases := map[string]struct {
		sub  messaging.Subscription
		want string
	}{
		"no name":        {messaging.Subscription{}, "name is required"},
		"dotted name":    {messaging.Subscription{Name: "a.b"}, "must not contain"},
		"wildcard name":  {messaging.Subscription{Name: "a>"}, "must not contain"},
		"spaced name":    {messaging.Subscription{Name: "a b"}, "must not contain"},
		"slashed name":   {messaging.Subscription{Name: "a/b"}, "must not contain"},
		"backslash name": {messaging.Subscription{Name: "a\\b"}, "must not contain"},
		"empty type":     {messaging.Subscription{Name: "a", Types: []string{""}}, "empty type"},
		"wildcard type":  {messaging.Subscription{Name: "a", Types: []string{"a.*"}}, "must not contain"},
		"negative max":   {messaging.Subscription{Name: "a", MaxDeliver: -1}, "max deliver"},
		"negative wait":  {messaging.Subscription{Name: "a", AckWait: -1}, "ack wait"},
		"negative retry": {messaging.Subscription{Name: "a", RetryDelay: -1}, "retry delay"},
		"unknown start":  {messaging.Subscription{Name: "a", Start: messaging.StartNew + 1}, "start position"},
		"negative start": {messaging.Subscription{Name: "a", Start: -1}, "start position"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			err := c.sub.Validate()
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %v, want it to mention %q", err, c.want)
			}
		})
	}
}

func TestCheckType(t *testing.T) {
	for _, ok := range []string{"a", "lab.grant.approved", "a-b_c.D9"} {
		if err := messaging.CheckType(ok); err != nil {
			t.Errorf("CheckType(%q): %v", ok, err)
		}
	}
	for _, bad := range []string{"", ".a", "a.", "a..b", "a.*", "a.>", "a>b", "a b", "a\tb", "a\nb"} {
		if err := messaging.CheckType(bad); err == nil {
			t.Errorf("CheckType(%q) accepted a type no broker can route", bad)
		}
	}
}

func TestMatches(t *testing.T) {
	all := messaging.Subscription{Name: "a"}
	if !all.Matches("anything") {
		t.Error("an empty filter must match every type")
	}
	some := messaging.Subscription{Name: "a", Types: []string{"x", "y"}}
	if !some.Matches("y") || some.Matches("z") {
		t.Error("the filter must match exactly its types")
	}
}

// Normalize defaults AckWait and puts Types in one order without repeats,
// so subscriptions that mean the same thing compare equal.
func TestNormalize(t *testing.T) {
	a := messaging.Subscription{Name: "n", Types: []string{"b", "a", "a"}}.Normalize()
	b := messaging.Subscription{Name: "n", Types: []string{"a", "b"}, AckWait: messaging.DefaultAckWait}.Normalize()
	if !reflect.DeepEqual(a, b) {
		t.Errorf("Normalize = %+v and %+v, want them equal", a, b)
	}
	if got := (messaging.Subscription{Name: "n", Types: []string{}}).Normalize().Types; got != nil {
		t.Errorf("empty Types normalized to %#v, want nil", got)
	}
}

// The attempt a provider puts on a context reads back; a context no
// provider gave reads 0.
func TestAttempt(t *testing.T) {
	if got := messaging.Attempt(context.Background()); got != 0 {
		t.Errorf("Attempt of a bare context = %d, want 0", got)
	}
	if got := messaging.Attempt(messaging.WithAttempt(context.Background(), 3)); got != 3 {
		t.Errorf("Attempt = %d, want 3", got)
	}
}

// Encode rejects an event that event.Encode rejects, and one whose type a
// broker cannot route.
func TestEncode(t *testing.T) {
	if _, _, err := messaging.Encode(event.Event{ID: "1", Source: "/t", Type: "a.b"}); err != nil {
		t.Errorf("Encode of a valid event: %v", err)
	}
	if _, _, err := messaging.Encode(event.Event{ID: "1", Type: "a"}); err == nil {
		t.Error("Encode accepted an event with no source")
	}
	if _, _, err := messaging.Encode(event.Event{ID: "1", Source: "/t", Type: "a.*"}); err == nil {
		t.Error("Encode accepted a type no broker can route")
	}
}
