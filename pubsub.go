package bus

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const retentionDuration = 24 * time.Hour

var _ Queue[proto.Message] = (*PubSub[*dummyEvent])(nil)

const ackDeadline = 20 * time.Second

type PubSub[T proto.Message] struct {
	inner        *pubsub.Client
	topic        *pubsub.Topic        // FIXME: This is not defined
	subscription *pubsub.Subscription // FIXME: This is not defined
}

func New[T proto.Message](ctx context.Context, projectID string) (*PubSub[T], func(context.Context) error, error) {
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, nil, fmt.Errorf("pubsub.NewClient: %w", err)
	}

	return &PubSub[T]{ //nolint:exhaustruct
			inner: client,
		}, func(ctx context.Context) error {
			if err := client.Close(); err != nil {
				return fmt.Errorf("client.Close: %w", err)
			}

			return nil
		}, nil
}

func (ps *PubSub[T]) CreateTopic(ctx context.Context, name string) (*pubsub.Topic, error) {
	topic := ps.inner.Topic(name)

	exists, err := topic.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("topic.Exists: %w", err)
	}

	if exists {
		return topic, nil
	}

	topic, err = ps.inner.CreateTopicWithConfig(ctx, name, &pubsub.TopicConfig{ //nolint:exhaustruct
		RetentionDuration: retentionDuration,
	})
	if err != nil {
		return nil, fmt.Errorf("ps.inner.CreateTopic: %w", err)
	}

	topic.EnableMessageOrdering = true

	return topic, nil
}

func (ps *PubSub[T]) CreateSubscription(ctx context.Context, topic *pubsub.Topic,
	name string,
) (*pubsub.Subscription, error) {
	subscription := ps.inner.Subscription(name)

	exists, err := subscription.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("subsciption.Exists: %w", err)
	}

	if exists {
		return subscription, nil
	}

	subscription, err = ps.inner.CreateSubscription(ctx, name, pubsub.SubscriptionConfig{ //nolint:exhaustruct
		AckDeadline:               ackDeadline,
		EnableExactlyOnceDelivery: true,
		EnableMessageOrdering:     true,
		ExpirationPolicy:          24 * time.Hour, //nolint:mnd
		RetentionDuration:         retentionDuration,
		Topic:                     topic,
	})
	if err != nil {
		return nil, fmt.Errorf("ps.inner.CreateSubscription: %w", err)
	}

	return subscription, nil
}

func (ps *PubSub[T]) Pub(ctx context.Context, message proto.Message) error {
	data, err := protojson.Marshal(message)
	if err != nil {
		return fmt.Errorf("protojson.Marshal: %w", err)
	}

	res := ps.topic.Publish(ctx, &pubsub.Message{ //nolint:exhaustruct
		Data:        data,
		OrderingKey: "event", // FIXME: This has to be variable
	})

	_, err = res.Get(ctx)
	if err != nil {
		return fmt.Errorf("res.Get: %w", err)
	}

	return nil
}

func (ps *PubSub[T]) Sub(ctx context.Context) <-chan MessageOrError[proto.Message] {
	ch := make(chan MessageOrError[proto.Message])

	go func() {
		err := ps.subscription.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			var message proto.Message

			if err := protojson.Unmarshal(m.Data, message); err != nil {
				ch <- MessageOrError[proto.Message]{Error: err} //nolint:exhaustruct

				m.Nack()

				return
			}

			ch <- MessageOrError[proto.Message]{Data: message} //nolint:exhaustruct

			m.Ack()
		})
		if err != nil {
			ch <- MessageOrError[proto.Message]{Error: err} //nolint:exhaustruct
		}
	}()

	return ch
}
