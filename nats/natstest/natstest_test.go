package natstest_test

import (
	"testing"

	"github.com/standards-lab/go-messaging/nats/natstest"
)

// fatalTB is a testing.TB whose Fatal is recorded rather than ending the
// test, so a test can watch Open fail one. Open uses no other method that
// fails or ends a test.
type fatalTB struct {
	testing.TB
	fatal []any
}

func (f *fatalTB) Fatal(args ...any) { f.fatal = append(f.fatal, args...) }

// Open fails the test when MESSAGING_NATS_URL is unset, and returns it when
// it is set.
func TestOpen(t *testing.T) {
	t.Run("unset", func(t *testing.T) {
		t.Setenv("MESSAGING_NATS_URL", "")
		f := &fatalTB{TB: t}
		natstest.Open(f)
		if f.fatal == nil {
			t.Error("Open did not fail the test with MESSAGING_NATS_URL unset")
		}
		t.Logf("Fatal: %v", f.fatal)
	})
	t.Run("set", func(t *testing.T) {
		t.Setenv("MESSAGING_NATS_URL", "nats://example:4222")
		f := &fatalTB{TB: t}
		if got := natstest.Open(f); got != "nats://example:4222" {
			t.Errorf("Open = %q, want the variable's URL", got)
		}
		if f.fatal != nil {
			t.Errorf("Open failed the test: %v", f.fatal)
		}
	})
}
