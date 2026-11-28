package nats

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/standards-lab/go-core/config"
	"github.com/standards-lab/go-messaging"
)

// QueueConfig names the NATS server a work queue connects to, its stream,
// and the subjects on it. It is also a service's queue configuration
// block: its JSON form, [QueueConfig.Merge], and [QueueConfig.Finalize]
// follow go-core's config conventions.
//
// One queue on a stream is its owner, and provisions it; every other queue
// binds the stream as it finds it. MaxAge configures the stream, so only
// the owner sets it.
type QueueConfig struct {
	// URL is the NATS server; Finalize defaults it to DefaultURL.
	URL string `json:"url"`
	// Name is the connection's name, which the server reports; optional.
	Name string `json:"name"`
	// Stream is the stream's name: a token, as a worker's Name is.
	Stream string `json:"stream"`
	// Subject is the work subject, one or more tokens: Enqueue stores each
	// job on it, and a worker with no Subject takes its jobs from it.
	Subject string `json:"subject"`
	// DeadLetter is the queue's dead-letter subject, one or more tokens: a
	// worker's DeadLetter, and a dead-letter worker's Subject, is DeadLetter
	// or a subject under it. It must not be Subject, and Subject must not be
	// under it.
	DeadLetter string `json:"dead_letter"`
	// Owner makes the queue the stream's owner. It is a tri-state pointer,
	// so an overlay can set it to false: nil, as false, is not the owner.
	Owner *bool `json:"owner"`
	// MaxAge bounds the stream's retention: the stream discards a job older
	// than MaxAge, whether or not a worker has taken it. 0 keeps every job.
	// JetStream requires a positive MaxAge to be at least the deduplication
	// window, [messaging.DefaultDuplicates]. Only the owner sets it.
	MaxAge config.Duration `json:"max_age"`
}

// Merge overlays src's set fields onto the receiver: a non-empty string, a
// non-nil Owner, including one set to false, and a non-zero duration.
func (cfg *QueueConfig) Merge(src *QueueConfig) {
	if src == nil {
		return
	}
	for dst, v := range map[*string]string{
		&cfg.URL:        src.URL,
		&cfg.Name:       src.Name,
		&cfg.Stream:     src.Stream,
		&cfg.Subject:    src.Subject,
		&cfg.DeadLetter: src.DeadLetter,
	} {
		if v != "" {
			*dst = v
		}
	}
	if src.Owner != nil {
		cfg.Owner = src.Owner
	}
	if src.MaxAge != 0 {
		cfg.MaxAge = src.MaxAge
	}
}

// Finalize defaults URL to [DefaultURL], reads the block's environment
// overrides when a prefix is given (<PREFIX>_NATS_QUEUE_URL, …_NAME,
// …_STREAM, …_SUBJECT, …_DEAD_LETTER, …_OWNER, and …_MAX_AGE), and
// validates the result. …_OWNER is parsed as [strconv.ParseBool] parses it.
func (cfg *QueueConfig) Finalize(envPrefix string) error {
	if cfg.URL == "" {
		cfg.URL = DefaultURL
	}
	for key, dst := range map[string]*string{
		"url":         &cfg.URL,
		"name":        &cfg.Name,
		"stream":      &cfg.Stream,
		"subject":     &cfg.Subject,
		"dead_letter": &cfg.DeadLetter,
	} {
		if v := os.Getenv(queueEnvName(envPrefix, key)); v != "" {
			*dst = v
		}
	}
	if err := config.SetFromEnv(&cfg.Owner, queueEnvName(envPrefix, "owner"), strconv.ParseBool); err != nil {
		return fmt.Errorf("nats: queue config: %w", err)
	}
	name := queueEnvName(envPrefix, "max_age")
	if err := cfg.MaxAge.Set(os.Getenv(name)); err != nil {
		return fmt.Errorf("nats: queue config: %s: %w", name, err)
	}
	return cfg.Validate()
}

func queueEnvName(prefix, key string) string {
	return config.EnvName(prefix, "nats", "queue", key)
}

// Validate reports every way cfg is unusable. A non-owner that sets MaxAge
// is unusable: it would configure a stream it does not provision.
func (cfg QueueConfig) Validate() error {
	var errs []error
	if cfg.URL == "" {
		errs = append(errs, errors.New("url is required"))
	}
	if err := messaging.CheckName(cfg.Stream); err != nil {
		errs = append(errs, fmt.Errorf("stream: %w", err))
	}
	subjectOK, deadOK := true, true
	if err := messaging.CheckType(cfg.Subject); err != nil {
		errs = append(errs, fmt.Errorf("subject: %w", err))
		subjectOK = false
	}
	if err := messaging.CheckType(cfg.DeadLetter); err != nil {
		errs = append(errs, fmt.Errorf("dead_letter: %w", err))
		deadOK = false
	}
	if subjectOK && deadOK && within(cfg.Subject, cfg.DeadLetter) {
		errs = append(errs, fmt.Errorf("subject %q must not be the dead-letter subject %q or under it", cfg.Subject, cfg.DeadLetter))
	}
	if cfg.MaxAge < 0 {
		errs = append(errs, errors.New("max_age must not be negative"))
	}
	if cfg.owner() {
		if cfg.MaxAge > 0 && cfg.MaxAge.Duration() < messaging.DefaultDuplicates {
			errs = append(errs, fmt.Errorf("max_age %v must be at least the deduplication window, %v", cfg.MaxAge, messaging.DefaultDuplicates))
		}
	} else if cfg.MaxAge != 0 {
		errs = append(errs, errors.New("max_age is set only on the stream's owner"))
	}
	if len(errs) > 0 {
		return fmt.Errorf("nats: queue config: %w", errors.Join(errs...))
	}
	return nil
}

func (cfg QueueConfig) owner() bool {
	return cfg.Owner != nil && *cfg.Owner
}

func within(subject, root string) bool {
	return subject == root || strings.HasPrefix(subject, root+".")
}
