package postgres_test

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/standards-lab/go-messaging/postgres"
)

var released = map[int]struct {
	name, up, down string
	transactional  bool
}{
	1: {"messaging", "486c6abe9b02a2de4ab3ad0550dd8330785d9c20c5853c07c8d779f397309169", "e2474958b549a4c3e5265e64770fc4b7229cccf0cee478cc3eb2283f25b831a9", true},
	2: {"outbox_published", "4b0e354178242023f986bba494261fe4904bf8958cce14f4dc8319503f859788", "60226563c9568317ce82bd22787079838eee03c87146a3b2db1ca00849952e96", false},
	3: {"inbox_handled", "6d2d5113c6108e06275876ed1eaabd3eccbc8d74a0b99ad6dd71406460769dbc", "355af6d52dd1a31e367ca64ea6cc6a6141132b5e8ef36fd3ffe69654930ca69f", false},
}

func sum(text string) string {
	h := sha256.Sum256([]byte(text))
	return hex.EncodeToString(h[:])
}

func TestReleasedMigrations(t *testing.T) {
	set, err := postgres.Migrations()
	if err != nil {
		t.Fatalf("Migrations: %v", err)
	}
	seen := map[int]bool{}
	for _, m := range set.Migrations {
		seen[m.Version] = true
		want, ok := released[m.Version]
		if !ok {
			t.Errorf("version %d %s is not in the released table; add it with up %s and down %s", m.Version, m.Name, sum(m.Up), sum(m.Down))
			continue
		}
		if m.Name != want.name || sum(m.Up) != want.up || sum(m.Down) != want.down || m.Transactional != want.transactional {
			t.Errorf("released version %d changed: %s up %s down %s transactional %v, released %s up %s down %s transactional %v",
				m.Version, m.Name, sum(m.Up), sum(m.Down), m.Transactional, want.name, want.up, want.down, want.transactional)
		}
	}
	for version, want := range released {
		if !seen[version] {
			t.Errorf("released version %d %s is missing from the set", version, want.name)
		}
	}
}
