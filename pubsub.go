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
	defaultAckDeadline       = 20 * time.Second
	defaultExpirationPolicy  = 24 * time.Hour
	defaultRetentionDuration = 24 * time.Hour
)

var _ Queue[proto.Message] = (*PubSub[*dummyEvent])(nil)

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

	PubSub[T proto.Message] struct {
		inner        *pubsub.Client
		topic        *pubsub.Topic
		subscription *pubsub.Subscription // FIXME: This is not defined
	}
)

var DefaultOptionalConfigWithMessageOrdering = OptionalConfig{
	AckDeadline:               defaultAckDeadline,
	EnableExactlyOnceDelivery: true,
	ExpirationPolicy:          defaultExpirationPolicy,
	RetentionDuration:         defaultRetentionDuration,
}

func New[T proto.Message](ctx context.Context, cfg *Config,
	optCfg *OptionalConfig) (*PubSub[T], func(context.Context) error, error) {
	client, err := pubsub.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, nil, fmt.Errorf("pubsub.NewClient: %w", err)
	}

	ps := &PubSub[T]{ //nolint:exhaustruct
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

func (ps *PubSub[T]) createTopic(ctx context.Context, cfg *Config, optCfg *OptionalConfig) error {
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
			RetentionDuration: defaultRetentionDuration,
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

func (ps *PubSub[T]) createSubscription(ctx context.Context, cfg *Config, optCfg *OptionalConfig) error {
	subscription := ps.inner.Subscription(cfg.Subscription)

	exists, err := subscription.Exists(ctx)
	if err != nil {
		return fmt.Errorf("subsciption.Exists: %w", err)
	}

	if exists {
		ps.subscription = subscription

		return nil
	}

	subscription, err = ps.inner.CreateSubscription(ctx, cfg.Subscription, pubsub.SubscriptionConfig{ //nolint:exhaustruct
		AckDeadline:               defaultAckDeadline,
		EnableExactlyOnceDelivery: true,
		EnableMessageOrdering:     true,
		ExpirationPolicy:          24 * time.Hour, //nolint:mnd
		RetentionDuration:         defaultRetentionDuration,
		Topic:                     ps.topic,
	})
	if err != nil {
		return fmt.Errorf("CreateSubscription: %w", err)
	}

	ps.subscription = subscription

	return nil
}
