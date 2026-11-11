package messaging

import (
	"context"

	"github.com/standards-lab/go-core/reactor"
)

// Gate is a quiesce gate the runtime's reactors hold shared while they
// work, so an operation that holds it exclusively runs with them paused:
// go-core's [reactor.Gate] is one. Shared waits until the gate admits a
// shared holder and returns its release, or fails, holding nothing; a wait
// ctx ends returns ctx's error.
type Gate interface {
	Shared(ctx context.Context) (release func(), err error)
}

var _ Gate = (*reactor.Gate)(nil)
