package bus

import (
	"context"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type MessageOrError[T any] struct {
	Data  T
	Error error
}

type Queue[T any] interface {
	Pub(ctx context.Context, message T) error
	Sub(ctx context.Context) <-chan MessageOrError[T]
}

type dummyEvent struct {
	ID     string
	Source string
}

func (e *dummyEvent) ProtoReflect() protoreflect.Message { //nolint:ireturn
	panic("not implemented")
}
