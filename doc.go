// Package messaging is the standard tier's broker operations: publishing an
// event and subscribing to events as a reactor source.
//
// The package imports no broker and no engine: the postgres sub-module is
// the database engine for its outbox and inbox, and the nats sub-module is
// a broker provider.
package messaging
