package bus

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	defaultAckDeadline       = 30 * time.Second
	defaultExpirationPolicy  = 24 * time.Hour
	defaultRetentionDuration = 24 * time.Hour
)

var _ Queue[*dummyEvent] = (*PubSubQueue[*dummyEvent])(nil)

var (
	ErrEmptyProjectID    = errors.New("empty project ID")
	ErrEmptyTopic        = errors.New("empty topic")
	ErrEmptySubscription = errors.New("empty subscription name")
	ErrEmptyOrigin       = errors.New("empty origin")
)

type (
	Config struct {
		ProjectID    string
		Topic        string
		Subscription string
		// Origin defines origin attribute for each message and filters out self created messages.
		Origin string
		// Optional. If specified, message ordering will be enabled.
		OrderingKey string
		// Optional. If not specified, will default to 30 seconds.
		AckDeadline time.Duration
		// Optional. If not specified, will default to one day.
		ExpirationPolicy time.Duration
		// Optional. If not specified, will default to one day.
		RetentionDuration         time.Duration
		EnableExactlyOnceDelivery bool
	}

	PubSubQueue[T proto.Message] struct {
		inner        *pubsub.Client
		topic        *pubsub.Topic
		subscription *pubsub.Subscription
		origin       string
		orderingKey  string
	}
)

// NewPubSubQueue creates PubSub implementation of the Queue interface.
func NewPubSubQueue[T proto.Message](ctx context.Context,
	cfg *Config) (*PubSubQueue[T], func(context.Context) error, error) {

	if cfg.ProjectID == "" {
		return nil, nil, ErrEmptyProjectID
	}

	if cfg.Topic == "" {
		return nil, nil, ErrEmptyTopic
	}

	if cfg.Subscription == "" {
		return nil, nil, ErrEmptySubscription
	}

	if cfg.Origin == "" {
		return nil, nil, ErrEmptyOrigin
	}

	if cfg.AckDeadline == 0 {
		cfg.AckDeadline = defaultAckDeadline
	}

	if cfg.ExpirationPolicy == 0 {
		cfg.ExpirationPolicy = defaultExpirationPolicy
	}

	if cfg.RetentionDuration == 0 {
		cfg.RetentionDuration = defaultRetentionDuration
	}

	client, err := pubsub.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, nil, fmt.Errorf("pubsub.NewClient: %w", err)
	}

	ps := &PubSubQueue[T]{ //nolint:exhaustruct
		inner:  client,
		origin: cfg.Origin,
	}

	if err = ps.createTopic(ctx, cfg); err != nil {
		return nil, nil, fmt.Errorf("createTopic: %w", err)
	}

	if err = ps.createSubscription(ctx, cfg); err != nil {
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

	m := &pubsub.Message{
		Data: data,
		Attributes: map[string]string{
			"origin": ps.origin,
		},
	} //nolint:exhaustruct

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

				m.Ack()

				return
			}

			ch <- Message[T]{
				Data:  message,
				Ack:   m.Ack,
				Nack:  m.Nack,
				Error: nil,
			}
		})
		if err != nil {
			ch <- Message[T]{Error: err} //nolint:exhaustruct
		}
	}()

	return ch
}

func (ps *PubSubQueue[T]) createTopic(ctx context.Context, cfg *Config) error {
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
	case cfg.RetentionDuration != 0:
		topic, err = ps.inner.CreateTopicWithConfig(ctx, cfg.Topic, &pubsub.TopicConfig{ //nolint:exhaustruct
			RetentionDuration: cfg.RetentionDuration,
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

func (ps *PubSubQueue[T]) createSubscription(ctx context.Context, cfg *Config) error {
	subscription := ps.inner.Subscription(cfg.Subscription)

	exists, err := subscription.Exists(ctx)
	if err != nil {
		return fmt.Errorf("subscription.Exists: %w", err)
	}

	if exists {
		ps.subscription = subscription

		return nil
	}

	subscription, err = ps.inner.CreateSubscription(ctx, cfg.Subscription, cfg.toPubSub(ps.topic, cfg.OrderingKey != ""))
	if err != nil {
		return fmt.Errorf("CreateSubscription: %w", err)
	}

	ps.subscription = subscription

	return nil
}

func (c Config) toPubSub(topic *pubsub.Topic, enableMessageOrdering bool) pubsub.SubscriptionConfig {
	return pubsub.SubscriptionConfig{ //nolint:exhaustruct
		AckDeadline:               c.AckDeadline,
		EnableExactlyOnceDelivery: c.EnableExactlyOnceDelivery,
		EnableMessageOrdering:     enableMessageOrdering,
		ExpirationPolicy:          c.ExpirationPolicy,
		RetentionDuration:         c.RetentionDuration,
		Topic:                     topic,
		Filter:                    fmt.Sprintf("attributes.origin != %q", c.Origin),
	}
}
