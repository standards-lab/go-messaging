// Package natstest gives each test its own JetStream stream, so no test
// depends on another or on what the compose stack's server already holds.
//
// The package exports:
//
//   - [Open], which returns the server MESSAGING_NATS_URL names, and fails
//     the test when it is unset
//   - [Scratch], which returns a stream name and subject prefix unique to
//     the test on any server, and deletes the stream when the test ends
//
// [Open] serves the provider's own integration suite; [Scratch] also serves
// a service's harness, which points a service process at a server of its
// choosing. The names follow pgtest's: Open reads the environment, and
// Scratch takes the server. Scratch creates nothing: the broker the test
// points at the stream provisions it, and Scratch's cleanup, registered
// first, runs after that broker's own. A stream nothing created is
// tolerated.
package natstest
