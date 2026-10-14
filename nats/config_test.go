package nats_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/standards-lab/go-core/config"

	"github.com/standards-lab/go-messaging/nats"
)

// Each unusable config fails Validate and New, with an error naming what is
// wrong with it.
func TestConfigValidateRejects(t *testing.T) {
	owner := new(true)
	for name, tc := range map[string]struct {
		cfg  nats.Config
		want string
	}{
		"no url":                     {nats.Config{Stream: "s", Prefix: "p"}, "url"},
		"no stream":                  {nats.Config{URL: "u", Prefix: "p"}, "stream"},
		"dotted stream":              {nats.Config{URL: "u", Stream: "a.b", Prefix: "p"}, "stream"},
		"slashed stream":             {nats.Config{URL: "u", Stream: "a/b", Prefix: "p"}, "stream"},
		"no prefix":                  {nats.Config{URL: "u", Stream: "s"}, "prefix"},
		"wildcard prefix":            {nats.Config{URL: "u", Stream: "s", Prefix: "p.*"}, "prefix"},
		"negative duplicates":        {nats.Config{URL: "u", Stream: "s", Prefix: "p", Owner: owner, Duplicates: -1}, "duplicates must not be negative"},
		"negative max_age":           {nats.Config{URL: "u", Stream: "s", Prefix: "p", Owner: owner, MaxAge: -1}, "max_age must not be negative"},
		"max_age below default dups": {nats.Config{URL: "u", Stream: "s", Prefix: "p", Owner: owner, MaxAge: config.Duration(time.Minute)}, "max_age 1m0s must be at least the deduplication window, 2m0s"},
		"max_age below duplicates":   {nats.Config{URL: "u", Stream: "s", Prefix: "p", Owner: owner, Duplicates: config.Duration(time.Hour), MaxAge: config.Duration(time.Minute)}, "max_age 1m0s must be at least the deduplication window, 1h0m0s"},
		"non-owner duplicates":       {nats.Config{URL: "u", Stream: "s", Prefix: "p", Duplicates: config.Duration(time.Hour)}, "duplicates is set only on the stream's owner"},
		"non-owner max_age":          {nats.Config{URL: "u", Stream: "s", Prefix: "p", MaxAge: config.Duration(time.Hour)}, "max_age is set only on the stream's owner"},
		"owner false max_age":        {nats.Config{URL: "u", Stream: "s", Prefix: "p", Owner: new(false), MaxAge: config.Duration(time.Hour)}, "max_age is set only on the stream's owner"},
	} {
		t.Run(name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Validate = %v, want an error naming %q", err, tc.want)
			}
			if _, err := nats.New(tc.cfg); err == nil {
				t.Errorf("New accepted %+v", tc.cfg)
			}
		})
	}
}

// The owner sets the stream's configuration, a positive MaxAge at least its
// deduplication window; a non-owner sets none of it.
func TestConfigValidateAccepts(t *testing.T) {
	for name, cfg := range map[string]nats.Config{
		"non-owner":            {URL: "u", Stream: "s", Prefix: "p.q"},
		"owner, defaults":      {URL: "u", Stream: "s", Prefix: "p", Owner: new(true)},
		"owner, max_age":       {URL: "u", Stream: "s", Prefix: "p", Owner: new(true), MaxAge: config.Duration(2 * time.Minute)},
		"owner, both":          {URL: "u", Stream: "s", Prefix: "p", Owner: new(true), Duplicates: config.Duration(time.Second), MaxAge: config.Duration(time.Minute)},
		"owner, only dedup":    {URL: "u", Stream: "s", Prefix: "p", Owner: new(true), Duplicates: config.Duration(time.Hour)},
		"owner false, nothing": {URL: "u", Stream: "s", Prefix: "p", Owner: new(false)},
	} {
		if err := cfg.Validate(); err != nil {
			t.Errorf("%s: Validate = %v", name, err)
		}
	}
}

// Merge overlays each set field, an Owner set to false included, and leaves
// the rest.
func TestConfigMerge(t *testing.T) {
	cfg := nats.Config{URL: "base", Name: "base", Stream: "base", Prefix: "base", Owner: new(true), Duplicates: 1, MaxAge: 1}
	cfg.Merge(nil)
	cfg.Merge(&nats.Config{})
	if cfg.URL != "base" || cfg.Owner == nil || !*cfg.Owner || cfg.Duplicates != 1 || cfg.MaxAge != 1 {
		t.Fatalf("an empty overlay changed the config: %+v", cfg)
	}

	var overlay nats.Config
	if err := json.Unmarshal([]byte(`{"url":"u","name":"n","stream":"s","prefix":"p","owner":false,"duplicates":"2m","max_age":"1h"}`), &overlay); err != nil {
		t.Fatal(err)
	}
	cfg.Merge(&overlay)
	want := nats.Config{URL: "u", Name: "n", Stream: "s", Prefix: "p", Duplicates: config.Duration(2 * time.Minute), MaxAge: config.Duration(time.Hour)}
	if cfg.Owner == nil || *cfg.Owner {
		t.Errorf("Owner = %v, want an Owner set to false", cfg.Owner)
	}
	cfg.Owner = nil
	if cfg != want {
		t.Errorf("Merge = %+v, want %+v", cfg, want)
	}
}

// Finalize fills the default URL and validates; with no prefix it reads no
// environment, and an absent Owner is not the owner.
func TestConfigFinalizeDefaults(t *testing.T) {
	t.Setenv("NATS_URL", "nats://elsewhere:4222")
	cfg := nats.Config{Stream: "s", Prefix: "p"}
	if err := cfg.Finalize(""); err != nil {
		t.Fatal(err)
	}
	if cfg.URL != nats.DefaultURL {
		t.Errorf("URL = %q, want %q", cfg.URL, nats.DefaultURL)
	}
	if cfg.Owner != nil {
		t.Errorf("Owner = %v, want nil", *cfg.Owner)
	}

	cfg = nats.Config{Stream: "s", Prefix: "p", MaxAge: config.Duration(time.Hour)}
	if err := cfg.Finalize(""); err == nil {
		t.Error("Finalize accepted a non-owner's max_age")
	}
}

// Finalize applies the block's environment overrides over the merged file
// values, then validates.
func TestConfigFinalizeEnv(t *testing.T) {
	cfg := nats.Config{Stream: "file", Prefix: "file"}
	cfg.Merge(&nats.Config{Stream: "overlay", Name: "overlay"})
	t.Setenv("SVC_NATS_URL", "nats://env:4222")
	t.Setenv("SVC_NATS_NAME", "env")
	t.Setenv("SVC_NATS_PREFIX", "env.events")
	t.Setenv("SVC_NATS_OWNER", "true")
	t.Setenv("SVC_NATS_DUPLICATES", "1m")
	t.Setenv("SVC_NATS_MAX_AGE", "2h")
	if err := cfg.Finalize("svc"); err != nil {
		t.Fatal(err)
	}
	if cfg.Owner == nil || !*cfg.Owner {
		t.Errorf("Owner = %v, want true", cfg.Owner)
	}
	cfg.Owner = nil
	want := nats.Config{URL: "nats://env:4222", Name: "env", Stream: "overlay", Prefix: "env.events", Duplicates: config.Duration(time.Minute), MaxAge: config.Duration(2 * time.Hour)}
	if cfg != want {
		t.Errorf("Finalize = %+v, want %+v", cfg, want)
	}

	// SVC_NATS_OWNER=false makes the config a non-owner, which its
	// environment's MaxAge then fails.
	t.Setenv("SVC_NATS_OWNER", "false")
	if err := cfg.Finalize("svc"); err == nil || !strings.Contains(err.Error(), "max_age") {
		t.Errorf("Finalize = %v, want a non-owner's max_age rejected", err)
	}
}

// An override that doesn't parse, or one that fails validation, fails
// Finalize, naming the variable or the field.
func TestConfigFinalizeEnvRejects(t *testing.T) {
	for name, tc := range map[string]struct {
		key, value, want string
	}{
		"owner not a bool":     {"SVC_NATS_OWNER", "maybe", "SVC_NATS_OWNER"},
		"duplicates not a dur": {"SVC_NATS_DUPLICATES", "soon", "SVC_NATS_DUPLICATES"},
		"max_age not a dur":    {"SVC_NATS_MAX_AGE", "later", "SVC_NATS_MAX_AGE"},
		"dotted stream":        {"SVC_NATS_STREAM", "not.a.token", "stream"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			cfg := nats.Config{Stream: "s", Prefix: "p"}
			if err := cfg.Finalize("svc"); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Finalize = %v, want an error naming %q", err, tc.want)
			}
		})
	}
}
