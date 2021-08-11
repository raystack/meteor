//+build integration

package kafka_test

import (
	"context"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"testing"

	kafkaLib "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/internal/logger"
	"github.com/odpf/meteor/plugins/extractors/kafka"
	"github.com/odpf/meteor/plugins/testutils"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
)

var (
	broker = "localhost:9093"
)

func TestMain(m *testing.M) {
	var client *kafkaLib.AdminClient
	ctx := context.TODO()
	// client, err := kafkaLib.NewAdminClient(&kafkaLib.ConfigMap{
	// 	"bootstrap.servers": broker,
	// })
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// setup test
	opts := dockertest.RunOptions{
		Repository: "moeenz/docker-kafka-kraft",
		Tag:        "229159a4a45b",
		Env: []string{
			"KRAFT_CONTAINER_HOST_NAME=1",
		},
		ExposedPorts: []string{"9093"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9093": {
				{HostIP: "localhost", HostPort: "9093"},
			},
		},
	}
	retryFn := func(resource *dockertest.Resource) (err error) {
		// create client
		client, err = kafkaLib.NewAdminClient(&kafkaLib.ConfigMap{
			"bootstrap.servers": broker,
		})
		return err
	}
	err, purgeContainer := testutils.CreateContainer(opts, retryFn)
	if err != nil {
		log.Fatal(err)
	}

	// setup and populate kafka for testing
	if err := setup(ctx, client); err != nil {
		log.Fatal(err)
	}

	// run tests
	code := m.Run()

	// clean up test data
	if err := cleanUp(ctx, client); err != nil {
		log.Fatal(err)
	}
	client.Close()

	// purge container
	if err := purgeContainer(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

func TestExtractorExtract(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		err := newExtractor().Extract(context.TODO(), map[string]interface{}{
			"wrong-config": "wrong-value",
		}, make(chan interface{}))

		assert.Equal(t, extractor.InvalidConfigError{}, err)
	})

	t.Run("should return list of topic metadata", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		out := make(chan interface{})

		go func() {
			err := newExtractor().Extract(ctx, map[string]interface{}{
				"broker": broker,
			}, out)
			close(out)

			assert.Nil(t, err)
		}()

		// build results
		var results []meta.Topic
		for d := range out {
			topic, ok := d.(meta.Topic)
			if !ok {
				t.Fatal(errors.New("invalid metadata format"))
			}
			results = append(results, topic)
		}

		// assert results with expected data
		expected := []meta.Topic{
			{
				Urn:    "meteor-test-topic-1",
				Name:   "meteor-test-topic-1",
				Source: "kafka",
			},
			{
				Urn:    "meteor-test-topic-2",
				Name:   "meteor-test-topic-2",
				Source: "kafka",
			},
			{
				Urn:    "meteor-test-topic-3",
				Name:   "meteor-test-topic-3",
				Source: "kafka",
			},
		}
		// We need this function because the extractor cannot guarantee order
		// so comparing expected slice and result slice will not be consistant
		assertResults(t, expected, results)
	})
}

func setup(ctx context.Context, client *kafkaLib.AdminClient) (err error) {
	results, err := client.CreateTopics(ctx, []kafkaLib.TopicSpecification{
		{Topic: "meteor-test-topic-1", NumPartitions: 1},
		{Topic: "meteor-test-topic-2", NumPartitions: 1},
		{Topic: "meteor-test-topic-3", NumPartitions: 1},
	})
	if err != nil {
		return
	}
	// Have to manually check for results for any errors
	for _, res := range results {
		if res.Error.Code() != kafkaLib.ErrNoError {
			return res.Error
		}
	}

	return
}

func cleanUp(ctx context.Context, client *kafkaLib.AdminClient) (err error) {
	results, err := client.DeleteTopics(ctx, []string{
		"meteor-test-topic-1",
		"meteor-test-topic-2",
		"meteor-test-topic-3",
	})
	if err != nil {
		return
	}
	// Have to manually check for results for any errors
	for _, res := range results {
		if res.Error.Code() != kafkaLib.ErrNoError {
			return res.Error
		}
	}

	return
}

func newExtractor() *kafka.Extractor {
	return kafka.New(
		logger.NewWithWriter("info", ioutil.Discard),
	)
}

// This function compares two slices without concerning about the order
func assertResults(t *testing.T, expected []meta.Topic, result []meta.Topic) {
	assert.Len(t, result, len(expected))

	expectedMap := make(map[string]meta.Topic)
	for _, topic := range expected {
		expectedMap[topic.Urn] = topic
	}

	for _, topic := range result {
		assert.Contains(t, expectedMap, topic.Urn)
		assert.Equal(t, expectedMap[topic.Urn], topic)

		// delete entry to make sure there is no duplicate
		delete(expectedMap, topic.Urn)
	}
}
