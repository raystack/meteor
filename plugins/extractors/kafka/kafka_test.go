//go:build plugins
// +build plugins

package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"

	kafkaLib "github.com/IBM/sarama"
	"github.com/ory/dockertest/v3"
	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/extractors/kafka"
	"github.com/raystack/meteor/test/mocks"
	"github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
)

var (
	brokerHost      string
	urnScope        = "test-kafka"
	dockerAvailable bool
)

func TestMain(m *testing.M) {
	dockerAvailable = utils.CheckDockerAvailability()
	if !dockerAvailable {
		os.Exit(m.Run())
	}

	var broker *kafkaLib.Broker
	// setup test
	opts := dockertest.RunOptions{
		Repository: "moeenz/docker-kafka-kraft",
		Tag:        "229159a4a45b",
		Env: []string{
			"KRAFT_CONTAINER_HOST_NAME=1",
		},
		ExposedPorts: []string{"9093"},
	}

	retryFn := func(resource *dockertest.Resource) (err error) {
		brokerHost = resource.GetHostPort("9093/tcp")
		conn, err := kafkaLib.NewClient([]string{brokerHost}, nil)
		if err != nil {
			fmt.Printf("error creating client ")
			return
		}

		// healthcheck
		if len(conn.Brokers()) == 0 {
			err = errors.New("not ready")
			return
		}

		broker, err = conn.Controller()
		if err != nil {
			fmt.Printf("error fetching controller %s", err.Error())
			conn.Close()
			return
		}
		return
	}

	purgeContainer, err := utils.CreateContainer(opts, retryFn)
	if err != nil {
		log.Fatal(err)
	}

	// setup and populate kafka for testing
	if err := setup(broker); err != nil {
		log.Fatal(err)
	}

	// run tests
	code := m.Run()

	// purge container
	if err := purgeContainer(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

func TestInit(t *testing.T) {
	utils.SkipIfNoDocker(t, dockerAvailable)
	t.Run("should return error for invalid config", func(t *testing.T) {
		err := newExtractor().Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"wrong-config": "wrong-value",
			},
		})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should return error for invalid cert file", func(t *testing.T) {
		err := newExtractor().Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"broker": brokerHost,
				"auth_config": map[string]any{
					"tls": map[string]any{
						"enabled":   "true",
						"cert_file": "non-existent-file",
						"key_file":  "non-existent-file",
						"ca_file":   "non-existent-file",
					},
				},
			},
		})

		assert.ErrorContains(t, err, "create cert")
	})

	t.Run("should return error for invalid ca cert", func(t *testing.T) {
		err := newExtractor().Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"broker": brokerHost,
				"auth_config": map[string]any{
					"tls": map[string]any{
						"enabled":   "true",
						"cert_file": "testdata/example-cert.txt",
						"key_file":  "testdata/example-key.txt",
						"ca_file":   "non-existent-file",
					},
				},
			},
		})

		assert.ErrorContains(t, err, "read ca cert file")
	})

	t.Run("should return error for create connection", func(t *testing.T) {
		err := newExtractor().Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"broker": brokerHost,
				"auth_config": map[string]any{
					"tls": map[string]any{
						"enabled":              "true",
						"insecure_skip_verify": "true",
						"cert_file":            "testdata/example-cert.txt",
						"key_file":             "testdata/example-key.txt",
						"ca_file":              "testdata/example-ca-cert.txt",
					},
				},
			},
		})

		assert.ErrorContains(t, err, "failed to create kafka consumer")
	})
}

func TestExtract(t *testing.T) {
	utils.SkipIfNoDocker(t, dockerAvailable)
	t.Run("should emit list of topic metadata", func(t *testing.T) {
		ctx := context.TODO()
		extr := newExtractor()
		err := extr.Init(ctx, plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"broker": brokerHost,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)
		assert.NoError(t, err)

		expected := []*meteorv1beta1.Entity{
			models.NewEntity("urn:kafka:test-kafka:topic:meteor-test-topic-1", "topic", "meteor-test-topic-1", "kafka", map[string]any{
				"number_of_partitions": float64(1),
			}),
			models.NewEntity("urn:kafka:test-kafka:topic:meteor-test-topic-2", "topic", "meteor-test-topic-2", "kafka", map[string]any{
				"number_of_partitions": float64(1),
			}),
			models.NewEntity("urn:kafka:test-kafka:topic:meteor-test-topic-3", "topic", "meteor-test-topic-3", "kafka", map[string]any{
				"number_of_partitions": float64(1),
			}),
		}

		utils.AssertEqualProtos(t, expected, utils.SortedEntities(emitter.GetAllEntities()))
	})
}

func setup(broker *kafkaLib.Broker) (err error) {

	// create client connection to create topics
	conn, err := kafkaLib.NewClient([]string{brokerHost}, nil)
	if err != nil {
		fmt.Printf("error creating client ")
		return
	}

	defer conn.Close()

	// create topics
	topicConfigs := map[string]*kafkaLib.TopicDetail{
		"meteor-test-topic-1": {NumPartitions: 1, ReplicationFactor: 1},
		"meteor-test-topic-2": {NumPartitions: 1, ReplicationFactor: 1},
		"meteor-test-topic-3": {NumPartitions: 1, ReplicationFactor: 1},
		"__consumer_offsets":  {NumPartitions: 1, ReplicationFactor: 1},
	}

	createTopicRequest := &kafkaLib.CreateTopicsRequest{TopicDetails: topicConfigs}
	_, err = broker.CreateTopics(createTopicRequest)
	if err != nil {
		fmt.Printf("error creating topics! %s", err.Error())
		return
	}

	return
}

func newExtractor() *kafka.Extractor {
	return kafka.New(utils.Logger)
}

// This function compares two slices without concerning about the order
func assertResults(t *testing.T, expected, result []models.Record) {
	assert.Len(t, result, len(expected))

	expectedMap := make(map[string]*meteorv1beta1.Entity)
	for _, record := range expected {
		entity := record.Entity()
		expectedMap[entity.GetUrn()] = entity
	}

	for _, record := range result {
		entity := record.Entity()
		assert.Contains(t, expectedMap, entity.GetUrn())
		assert.Equal(t, expectedMap[entity.GetUrn()], entity)

		delete(expectedMap, entity.GetUrn())
	}
}
