package bus

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var (
	once     sync.Once
	mu       sync.Mutex
	count    int
	pool     *dockertest.Pool
	resource *dockertest.Resource
	endpoint string
)

func TestCreateTopic(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	defer setUp(ctx, t)()

	client, err := pubsub.NewClient(ctx, "test-internal-retention", option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("failed creating pubsub client: %v", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Errorf("failed closing client: %v", err)
		}
	}()

	ps := &PubSubQueue[string]{
		inner:  client,
		origin: "test-origin",
	}

	cfg := &Config{
		ProjectID:         "test-internal-retention",
		Topic:             "internal-topic-with-retention",
		RetentionDuration: 72 * time.Hour,
	}

	topic, err := ps.createTopic(ctx, cfg)
	if err != nil {
		t.Fatalf("failed creating topic failed: %v", err)
	}

	if topic == nil {
		t.Fatal("want topic to be non-nil")
	}

	wantName := fmt.Sprintf("projects/%s/topics/%s", cfg.ProjectID, cfg.Topic)
	if topic.Name != wantName {
		t.Errorf("want topic name: %q, got: %q", wantName, topic.Name)
	}

	if topic.MessageRetentionDuration.AsDuration() != cfg.RetentionDuration {
		t.Errorf("want retention duration: %v, got: %v", cfg.RetentionDuration, topic.MessageRetentionDuration.AsDuration())
	}

	topics := client.TopicAdminClient.ListTopics(ctx, &pubsubpb.ListTopicsRequest{
		Project: "projects/" + cfg.ProjectID,
	})

	var foundTopic *pubsubpb.Topic
	for {
		listedTopic, err := topics.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatalf("Failed to iterate topics: %v", err)
		}
		if listedTopic.Name == wantName {
			foundTopic = listedTopic
			break
		}
	}

	if foundTopic == nil {
		t.Fatal("topic should exist in PubSub after creation")
	}
}

func setUp(ctx context.Context, t *testing.T) func() {
	t.Helper()

	mu.Lock()
	defer mu.Unlock()

	count++

	once.Do(func() {
		var err error

		pool, err = dockertest.NewPool("")
		if err != nil {
			t.Fatalf("failed creating docker pool: %v", err)
		}

		options := &dockertest.RunOptions{
			Repository: "gcr.io/google.com/cloudsdktool/google-cloud-cli",
			Tag:        "emulators",
			Cmd: []string{
				"gcloud", "beta", "emulators", "pubsub", "start",
				"--host-port=0.0.0.0:8087",
			},
			ExposedPorts: []string{"8087/tcp"},
		}

		resource, err = pool.RunWithOptions(options, func(config *docker.HostConfig) {
			config.AutoRemove = true
			config.RestartPolicy = docker.RestartPolicy{Name: "no"}
		})
		if err != nil {
			t.Fatalf("failed starting resource: %s", err)
		}

		endpoint = fmt.Sprintf("localhost:%s", resource.GetPort("8087/tcp"))

		if err = os.Setenv("PUBSUB_EMULATOR_HOST", endpoint); err != nil {
			t.Errorf("failed setting environment variable: %v", err)
		}

		pool.MaxWait = 60 * time.Second
		if err = pool.Retry(func() error {
			client, err := pubsub.NewClient(ctx, "test-project")
			if err != nil {
				return err
			}
			defer func() {
				if err := client.Close(); err != nil {
					t.Errorf("failed closing client: %v", err)
				}
			}()
			return nil
		}); err != nil {
			t.Fatalf("failed connecting to pubsub emulator: %s", err)
		}
	})

	return func() {
		mu.Lock()
		defer mu.Unlock()

		count--

		if count != 0 {
			return
		}

		if err := pool.Purge(resource); err != nil {
			t.Fatalf("purge: %v", err)
		}
	}
}
