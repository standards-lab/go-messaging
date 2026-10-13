// Package pgtest gives each test its own throwaway Postgres database, so no
// test depends on the compose stack's database or on another test.
//
// The package exports:
//
//   - [Scratch], which creates an empty database on any administrative
//     connection and returns its name
//   - [Open], which creates one on the server MESSAGING_DSN names and
//     returns a session over it
//   - [DSN], which points an administrative connection string at a scratch
//     database, and [Pool], which opens a pool on one
//
// [Scratch] serves a service's domain tests and the harness that points a
// service process at its database; [Open] serves the engine's own
// integration suite. Each database is dropped when the test ends.
package pgtest
