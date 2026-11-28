// Package natstest gives each test its own JetStream stream, so no test
// depends on another or on what the compose stack's server already holds.
//
// The package exports:
//
//   - [Open], which returns the server MESSAGING_NATS_URL names, and fails
//     the test when it is unset
//   - [Scratch], which returns a stream name and type namespace, the
//     broker's prefix, unique to the test on any server, and deletes the
//     stream when the test ends; the test's event types are under the
//     namespace
//   - [ScratchQueue], which returns a stream name and a work queue's work
//     and dead-letter subjects, unique to the test, and deletes the stream
//     when the test ends
//
// [Open] serves the provider's own integration suite; [Scratch] and
// [ScratchQueue] also serve a service's harness, which points a service
// process at a server of its choosing. Open reads the environment, and
// Scratch and ScratchQueue take the server. Neither creates anything: the
// broker or queue the test points at the stream provisions it, and the
// cleanup, registered first, runs after that broker's or queue's own. A
// stream nothing created is tolerated.
package natstest
