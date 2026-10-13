package messaging

import (
	"errors"
	"fmt"
	"os"
	"time"

	libconfig "github.com/standards-lab/go-core/config"
)

const (
	// DefaultRelayPoll is how long the relay waits between passes once the
	// outbox is empty, when a configuration sets none.
	DefaultRelayPoll = 250 * time.Millisecond
	// DefaultRelayTimeout bounds the relay's publish of one event, when a
	// configuration sets none.
	DefaultRelayTimeout = 10 * time.Second
)

// Config is a service's messaging block, the part of its messaging that
// names no broker: the provider's own block sits beside it. Source is the service's CloudEvents source, one value for
// every replica. RelayPoll is how long the relay waits between passes once
// the outbox is empty. RelayTimeout bounds the relay's publish of one
// event, and so how long a hung publish holds the event's outbox row; it is
// the relay's outbox.Timeout.
type Config struct {
	Source       string             `json:"source"`
	RelayPoll    libconfig.Duration `json:"relay_poll"`
	RelayTimeout libconfig.Duration `json:"relay_timeout"`
}

// Merge overlays src's set fields onto the receiver.
func (c *Config) Merge(src *Config) {
	if src == nil {
		return
	}
	if src.Source != "" {
		c.Source = src.Source
	}
	if src.RelayPoll != 0 {
		c.RelayPoll = src.RelayPoll
	}
	if src.RelayTimeout != 0 {
		c.RelayTimeout = src.RelayTimeout
	}
}

// Finalize defaults RelayPoll to [DefaultRelayPoll] and RelayTimeout to
// [DefaultRelayTimeout], reads the block's environment overrides when a
// prefix is given (<PREFIX>_MESSAGING_SOURCE, …_RELAY_POLL, and
// …_RELAY_TIMEOUT), and validates the result.
func (c *Config) Finalize(envPrefix string) error {
	if c.RelayPoll == 0 {
		c.RelayPoll = libconfig.Duration(DefaultRelayPoll)
	}
	if c.RelayTimeout == 0 {
		c.RelayTimeout = libconfig.Duration(DefaultRelayTimeout)
	}
	if envPrefix != "" {
		if v := os.Getenv(libconfig.EnvName(envPrefix, "messaging_source")); v != "" {
			c.Source = v
		}
		for _, o := range []struct {
			part string
			d    *libconfig.Duration
		}{
			{"messaging_relay_poll", &c.RelayPoll},
			{"messaging_relay_timeout", &c.RelayTimeout},
		} {
			name := libconfig.EnvName(envPrefix, o.part)
			if err := o.d.Set(os.Getenv(name)); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
		}
	}
	return c.Validate()
}

// Validate reports every way c is unusable: the source must be set, and
// the poll and the timeout positive.
func (c Config) Validate() error {
	var errs []error
	if c.Source == "" {
		errs = append(errs, errors.New("source is required"))
	}
	if c.RelayPoll <= 0 {
		errs = append(errs, fmt.Errorf("relay_poll must be positive, got %s", c.RelayPoll))
	}
	if c.RelayTimeout <= 0 {
		errs = append(errs, fmt.Errorf("relay_timeout must be positive, got %s", c.RelayTimeout))
	}
	return errors.Join(errs...)
}
