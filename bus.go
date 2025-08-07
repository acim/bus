package bus

import (
	"context"
)

type Message[T any] struct {
	Data  *T
	Error error
	Ack   func()
	Nack  func()
}

// Queue defines message queue methods.
type Queue[T any] interface {
	Pub(ctx context.Context, message *T) error
	Sub() <-chan *Message[T]
}
