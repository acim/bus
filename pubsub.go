package bus

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
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
		inner       *pubsub.Client
		publisher   *pubsub.Publisher
		subscriber  *pubsub.Subscriber
		origin      string
		orderingKey string
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

	topic, err := ps.createTopic(ctx, cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("createTopic: %w", err)
	}

	ps.publisher = ps.inner.Publisher(topic.GetName())

	subscription, err := ps.createSubscription(ctx, cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("createSubscription: %w", err)
	}

	ps.subscriber = ps.inner.Subscriber(subscription.GetName())

	return ps, func(ctx context.Context) error {
		ps.publisher.Stop()

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

	_ = ps.publisher.Publish(ctx, m)

	return nil
}

// Sub implements Queue interface.
func (ps *PubSubQueue[T]) Sub(ctx context.Context) <-chan Message[T] {
	ch := make(chan Message[T])

	go func() {
		err := ps.subscriber.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
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

func (ps *PubSubQueue[T]) createTopic(ctx context.Context, cfg *Config) (*pubsubpb.Topic, error) {
	topics := ps.inner.TopicAdminClient.ListTopics(ctx, &pubsubpb.ListTopicsRequest{
		Project: "projects/" + cfg.ProjectID,
	})

	for {
		topic, err := topics.Next()
		if err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}

			return nil, fmt.Errorf("topics.Next(): %w", err)
		}

		if topic.Name == cfg.Topic {
			return topic, nil
		}
	}

	switch {
	case cfg.RetentionDuration != 0:
		topic, err := ps.inner.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{ //nolint:exhaustruct
			Name:                     "projects/" + cfg.ProjectID + "/topics/" + cfg.Topic,
			MessageRetentionDuration: durationpb.New(cfg.RetentionDuration),
		})
		if err != nil {
			return nil, fmt.Errorf("TopicAdminClient.CreateTopic: %w", err)
		}

		return topic, nil
	default:
		topic, err := ps.inner.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{ //nolint:exhaustruct
			Name: "projects/" + cfg.ProjectID + "/topics/" + cfg.Topic,
		})
		if err != nil {
			return nil, fmt.Errorf("TopicAdminClient.CreateTopic: %w", err)
		}

		return topic, nil
	}
}

func (ps *PubSubQueue[T]) createSubscription(ctx context.Context, cfg *Config) (*pubsubpb.Subscription, error) {
	subscriptions := ps.inner.SubscriptionAdminClient.ListSubscriptions(ctx, &pubsubpb.ListSubscriptionsRequest{
		Project: "projects/" + cfg.ProjectID,
	})

	for {
		subscription, err := subscriptions.Next()
		if err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}

			return nil, fmt.Errorf("subscriptions.Next(): %w", err)
		}

		if subscription.Name == cfg.Subscription {
			return subscription, nil
		}
	}

	subscription, err := ps.inner.SubscriptionAdminClient.CreateSubscription(ctx, cfg.toPubSub(cfg.OrderingKey != ""))
	if err != nil {
		return nil, fmt.Errorf("CreateSubscription: %w", err)
	}

	return subscription, nil
}

func (c Config) toPubSub(enableMessageOrdering bool) *pubsubpb.Subscription {
	return &pubsubpb.Subscription{ //nolint:exhaustruct
		Name:                      "projects/" + c.ProjectID + "/subscriptions/" + c.Subscription,
		Topic:                     "projects/" + c.ProjectID + "/topics/" + c.Topic,
		AckDeadlineSeconds:        int32(c.AckDeadline.Seconds()),
		EnableExactlyOnceDelivery: c.EnableExactlyOnceDelivery,
		EnableMessageOrdering:     enableMessageOrdering,
		ExpirationPolicy:          &pubsubpb.ExpirationPolicy{Ttl: durationpb.New(c.ExpirationPolicy)},
		MessageRetentionDuration:  durationpb.New(c.RetentionDuration),
		Filter:                    fmt.Sprintf("attributes.origin != %q", c.Origin),
	}
}
