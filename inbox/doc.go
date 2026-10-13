// Package inbox is a consumer's guard against handling a redelivered event
// twice, on any SQL engine that supplies its statement.
//
// The package exports:
//
//   - [Engine], the statement the inbox runs, compiled for one engine
//   - [Inbox], the inbox on one engine's statement, with [New], which checks
//     the engine and returns one, and [Inbox.Claim], which records a
//     consumer's handling of an event and reports whether it is the first
//
// Delivery is at least once, so a handler can see an event again. An
// [Inbox] is built with [New] over an [Engine], the statement one engine's
// module compiles. One such module is go-messaging's postgres sub-module,
// which ships the inbox's table in the same migration set as the outbox's.
// [Inbox.Claim] records, in the handler's own transaction, that a consumer
// is handling an event, and reports whether it is the first time. The claim
// commits with the handler's work or rolls back with it, so when a handler
// fails, its redelivery can claim the event again. This package holds no SQL
// and imports no driver.
package inbox
