package bus

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	DefaultAckDeadline       = 30 * time.Second
	DefaultExpirationPolicy  = 24 * time.Hour
	DefaultRetentionDuration = 24 * time.Hour
)

var _ Queue[*dummyEvent] = (*PubSubQueue[*dummyEvent])(nil)

type (
	Config struct {
		ProjectID    string
		Topic        string
		Subscription string
		// Optional. If specified, message ordering will be enabled.
		OrderingKey string
	}

	OptionalConfig struct {
		AckDeadline               time.Duration
		EnableExactlyOnceDelivery bool
		ExpirationPolicy          time.Duration
		RetentionDuration         time.Duration
	}

	PubSubQueue[T proto.Message] struct {
		inner        *pubsub.Client
		topic        *pubsub.Topic
		subscription *pubsub.Subscription
		orderingKey  string
	}
)

var DefaultOptionalConfigWithMessageOrdering = OptionalConfig{
	AckDeadline:               DefaultAckDeadline,
	EnableExactlyOnceDelivery: true,
	ExpirationPolicy:          DefaultExpirationPolicy,
	RetentionDuration:         DefaultRetentionDuration,
}

// NewPubSubQueue creates PubSub implementation of the Queue interface.
func NewPubSubQueue[T proto.Message](ctx context.Context, cfg *Config,
	optCfg *OptionalConfig) (*PubSubQueue[T], func(context.Context) error, error) {
	client, err := pubsub.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, nil, fmt.Errorf("pubsub.NewClient: %w", err)
	}

	ps := &PubSubQueue[T]{ //nolint:exhaustruct
		inner: client,
	}

	if err = ps.createTopic(ctx, cfg, optCfg); err != nil {
		return nil, nil, fmt.Errorf("createTopic: %w", err)
	}

	if err = ps.createSubscription(ctx, cfg, optCfg); err != nil {
		return nil, nil, fmt.Errorf("createSubscription: %w", err)
	}

	return ps, func(ctx context.Context) error {
		if err := client.Close(); err != nil {
			return fmt.Errorf("client.Close: %w", err)
		}

		return nil
	}, nil
}

// Pub implements Queue interface.
func (ps *PubSubQueue[T]) Pub(ctx context.Context, message T) error {
	data, err := protojson.Marshal(message)
	if err != nil {
		return fmt.Errorf("protojson.Marshal: %w", err)
	}

	m := &pubsub.Message{Data: data} //nolint:exhaustruct

	if ps.orderingKey != "" {
		m.OrderingKey = ps.orderingKey
	}

	_ = ps.topic.Publish(ctx, m)

	return nil
}

// Sub implements Queue interface.
func (ps *PubSubQueue[T]) Sub(ctx context.Context) <-chan Message[T] {
	ch := make(chan Message[T])

	go func() {
		err := ps.subscription.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			var message T

			if err := protojson.Unmarshal(m.Data, message); err != nil {
				ch <- Message[T]{Error: err} //nolint:exhaustruct

				m.Nack()

				return
			}

			ch <- Message[T]{Data: message} //nolint:exhaustruct

			m.Ack()
		})
		if err != nil {
			ch <- Message[T]{Error: err} //nolint:exhaustruct
		}
	}()

	return ch
}

func (ps *PubSubQueue[T]) createTopic(ctx context.Context, cfg *Config, optCfg *OptionalConfig) error {
	topic := ps.inner.Topic(cfg.Topic)

	exists, err := topic.Exists(ctx)
	if err != nil {
		return fmt.Errorf("topic.Exists: %w", err)
	}

	if exists {
		ps.topic = topic

		return nil
	}

	switch {
	case optCfg != nil && optCfg.RetentionDuration != 0:
		topic, err = ps.inner.CreateTopicWithConfig(ctx, cfg.Topic, &pubsub.TopicConfig{ //nolint:exhaustruct
			RetentionDuration: DefaultRetentionDuration,
		})
		if err != nil {
			return fmt.Errorf("CreateTopicWithConfig: %w", err)
		}
	default:
		topic, err = ps.inner.CreateTopic(ctx, cfg.Topic)
		if err != nil {
			return fmt.Errorf("CreateTopic: %w", err)
		}
	}

	if cfg.OrderingKey != "" {
		topic.EnableMessageOrdering = true
	}

	ps.topic = topic

	return nil
}

func (ps *PubSubQueue[T]) createSubscription(ctx context.Context, cfg *Config, optCfg *OptionalConfig) error {
	subscription := ps.inner.Subscription(cfg.Subscription)

	exists, err := subscription.Exists(ctx)
	if err != nil {
		return fmt.Errorf("subsciption.Exists: %w", err)
	}

	if exists {
		ps.subscription = subscription

		return nil
	}

	subscription, err = ps.inner.CreateSubscription(ctx, cfg.Subscription,
		optCfg.toPubSub(ps.topic, cfg.OrderingKey != ""))
	if err != nil {
		return fmt.Errorf("CreateSubscription: %w", err)
	}

	ps.subscription = subscription

	return nil
}

func (c OptionalConfig) toPubSub(topic *pubsub.Topic, enableMessageOrdering bool) pubsub.SubscriptionConfig {
	return pubsub.SubscriptionConfig{ //nolint:exhaustruct
		AckDeadline:               c.AckDeadline,
		EnableExactlyOnceDelivery: c.EnableExactlyOnceDelivery,
		EnableMessageOrdering:     enableMessageOrdering,
		ExpirationPolicy:          c.ExpirationPolicy,
		RetentionDuration:         c.RetentionDuration,
		Topic:                     topic,
	}
}
