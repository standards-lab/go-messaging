package pgtest

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"net/url"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
	"github.com/standards-lab/sqlate"
	"github.com/standards-lab/sqlate/postgres"
)

// Open creates a uniquely named database and returns a session over a pool
// connected to it. A missing MESSAGING_DSN fails the test: the integration
// tag states that the stack is expected.
func Open(t testing.TB) *sqlate.DB {
	t.Helper()
	admin := os.Getenv("MESSAGING_DSN")
	if admin == "" {
		t.Fatal("MESSAGING_DSN is not set; run under mise with the compose stack up")
	}
	dsn := DSN(t, admin, Scratch(t, admin, "messaging"))
	return sqlate.Wrap(Pool(t, dsn), postgres.Dialect{})
}

// Scratch creates a uniquely named, empty database, <prefix>_test_<hex>, on
// the server admin connects to, and returns its name. It drops the
// database when the test ends, ending any session still open on it, such
// as a service process's pool.
func Scratch(t testing.TB, admin, prefix string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := sql.Open("pgx", admin)
	if err != nil {
		t.Fatalf("open %s: %v", admin, err)
	}
	name := prefix + "_test_" + suffix(t)
	if _, err := db.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		// A create that timed out can still finish on the server, so the
		// database is dropped if it exists, on a context of its own.
		dctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, _ = db.ExecContext(dctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		cancel()
		_ = db.Close()
		t.Fatalf("create database %s: %v", name, err)
	}
	t.Cleanup(func() {
		// WITH (FORCE) ends the database's other sessions, and the server can
		// still report one closing, so the drop retries with a deadline each.
		var err error
		for range 4 {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_, err = db.ExecContext(ctx, "DROP DATABASE "+name+" WITH (FORCE)")
			cancel()
			if err == nil {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if err != nil {
			t.Errorf("drop database %s: %v", name, err)
		}
		_ = db.Close()
	})
	return name
}

// DSN is admin with its database replaced by name.
func DSN(t testing.TB, admin, name string) string {
	t.Helper()
	u, err := url.Parse(admin)
	if err != nil {
		t.Fatalf("parse %s: %v", admin, err)
	}
	u.Path = "/" + name
	return u.String()
}

// Pool opens a pool on dsn and pings it, closing it when the test ends.
func Pool(t testing.TB, dsn string) *sql.DB {
	t.Helper()
	pool, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", dsn, err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := pool.PingContext(ctx); err != nil {
		t.Fatalf("ping %s: %v", dsn, err)
	}
	return pool
}

// suffix returns eight random hex characters, so parallel packages never
// pick the same database name.
func suffix(t testing.TB) string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("random suffix: %v", err)
	}
	return hex.EncodeToString(b[:])
}
