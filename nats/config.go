package nats

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/standards-lab/go-core/config"
	"github.com/standards-lab/go-messaging"
)

// DefaultURL is the NATS server Finalize fills in when a configuration names
// none.
const DefaultURL = "nats://127.0.0.1:4222"

// Config names the NATS server a broker connects to and the stream it
// publishes to and subscribes on. It is also a service's nats configuration
// block: its JSON form, [Config.Merge], and [Config.Finalize] follow go-core's
// config conventions.
//
// One broker on a stream is its owner, and provisions it; every other broker
// binds the stream as it finds it. Duplicates and MaxAge configure the
// stream, so only the owner sets them.
type Config struct {
	// URL is the NATS server; Finalize defaults it to DefaultURL.
	URL string `json:"url"`
	// Name is the connection's name, which the server reports; optional.
	Name string `json:"name"`
	// Stream is the stream's name: a token, as a subscription's Name is.
	Stream string `json:"stream"`
	// Prefix is the namespace of the broker's event types, one or more
	// tokens. An event is published with its type as the subject, so every
	// type the broker publishes or subscribes to starts with Prefix and a
	// further token, and the stream captures Prefix.>.
	Prefix string `json:"prefix"`
	// Owner makes the broker the stream's owner. It is a tri-state pointer,
	// so an overlay can set it to false: nil, as false, is not the owner.
	Owner *bool `json:"owner"`
	// Duplicates is the stream's deduplication window; 0 is
	// [messaging.DefaultDuplicates]. Only the owner sets it.
	Duplicates config.Duration `json:"duplicates"`
	// MaxAge bounds the stream's retention: the stream discards an event
	// older than MaxAge, whether or not every consumer has received it. A
	// consumer that falls further behind than MaxAge recovers when the
	// event's producer republishes its current state. 0 keeps every event.
	// JetStream requires a positive MaxAge to be at least the deduplication
	// window. Only the owner sets it.
	MaxAge config.Duration `json:"max_age"`
}

// Merge overlays src's set fields onto the receiver: a non-empty string, a
// non-nil Owner, including one set to false, and a non-zero duration.
func (cfg *Config) Merge(src *Config) {
	if src == nil {
		return
	}
	if src.URL != "" {
		cfg.URL = src.URL
	}
	if src.Name != "" {
		cfg.Name = src.Name
	}
	if src.Stream != "" {
		cfg.Stream = src.Stream
	}
	if src.Prefix != "" {
		cfg.Prefix = src.Prefix
	}
	if src.Owner != nil {
		cfg.Owner = src.Owner
	}
	if src.Duplicates != 0 {
		cfg.Duplicates = src.Duplicates
	}
	if src.MaxAge != 0 {
		cfg.MaxAge = src.MaxAge
	}
}

// Finalize defaults URL to [DefaultURL], reads the block's environment
// overrides when a prefix is given (<PREFIX>_NATS_URL, …_NAME, …_STREAM,
// …_PREFIX, …_OWNER, …_DUPLICATES, and …_MAX_AGE), and validates the result.
// …_OWNER is parsed as [strconv.ParseBool] parses it.
func (cfg *Config) Finalize(envPrefix string) error {
	if cfg.URL == "" {
		cfg.URL = DefaultURL
	}
	for key, dst := range map[string]*string{
		"url":    &cfg.URL,
		"name":   &cfg.Name,
		"stream": &cfg.Stream,
		"prefix": &cfg.Prefix,
	} {
		if v := os.Getenv(envName(envPrefix, key)); v != "" {
			*dst = v
		}
	}
	if err := config.SetFromEnv(&cfg.Owner, envName(envPrefix, "owner"), strconv.ParseBool); err != nil {
		return fmt.Errorf("nats: config: %w", err)
	}
	for key, dst := range map[string]*config.Duration{
		"duplicates": &cfg.Duplicates,
		"max_age":    &cfg.MaxAge,
	} {
		name := envName(envPrefix, key)
		if err := dst.Set(os.Getenv(name)); err != nil {
			return fmt.Errorf("nats: config: %s: %w", name, err)
		}
	}
	return cfg.Validate()
}

// envName is the environment variable that overrides the block's key under
// prefix, or "" for no prefix, which no variable is read under.
func envName(prefix, key string) string {
	return config.EnvName(prefix, "nats", key)
}

// Validate reports every way cfg is unusable. A non-owner that sets
// Duplicates or MaxAge is unusable: it would configure a stream it does not
// provision.
func (cfg Config) Validate() error {
	var errs []error
	if cfg.URL == "" {
		errs = append(errs, errors.New("url is required"))
	}
	if err := messaging.CheckName(cfg.Stream); err != nil {
		errs = append(errs, fmt.Errorf("stream: %w", err))
	}
	if err := messaging.CheckType(cfg.Prefix); err != nil {
		errs = append(errs, fmt.Errorf("prefix: %w", err))
	}
	if cfg.Duplicates < 0 {
		errs = append(errs, errors.New("duplicates must not be negative"))
	}
	if cfg.MaxAge < 0 {
		errs = append(errs, errors.New("max_age must not be negative"))
	}
	if cfg.owner() {
		if window := cfg.window(); cfg.MaxAge > 0 && cfg.MaxAge.Duration() < window {
			errs = append(errs, fmt.Errorf("max_age %v must be at least the deduplication window, %v", cfg.MaxAge, window))
		}
	} else {
		if cfg.Duplicates != 0 {
			errs = append(errs, errors.New("duplicates is set only on the stream's owner"))
		}
		if cfg.MaxAge != 0 {
			errs = append(errs, errors.New("max_age is set only on the stream's owner"))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("nats: config: %w", errors.Join(errs...))
	}
	return nil
}

// owner reports whether the broker owns the stream: Owner is set and true.
func (cfg Config) owner() bool {
	return cfg.Owner != nil && *cfg.Owner
}

// window is the stream's deduplication window: Duplicates, or
// [messaging.DefaultDuplicates] when it is 0.
func (cfg Config) window() time.Duration {
	if cfg.Duplicates == 0 {
		return messaging.DefaultDuplicates
	}
	return cfg.Duplicates.Duration()
}
