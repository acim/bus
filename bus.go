package bus

import (
	"context"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type Message[T proto.Message] struct {
	Data  T
	Error error
	Ack   func()
	Nack  func()
}

// Queue defines message queue methods.
type Queue[T proto.Message] interface {
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
