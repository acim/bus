package bus

import (
	"context"

	"google.golang.org/protobuf/reflect/protoreflect"
)

type Message[T any] struct {
	Data  T
	Error error
}

// Queue defines message queue methods.
type Queue[T any] interface {
	Pub(ctx context.Context, message T) error
	Sub(ctx context.Context) <-chan Message[T]
}

type dummyEvent struct {
	ID     string
	Source string
}

func (e *dummyEvent) ProtoReflect() protoreflect.Message { //nolint:ireturn
	panic("not implemented")
}
