//+build integration

package kafka_test

import (
	"context"
	"errors"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"testing"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/kafka"
	"github.com/odpf/meteor/plugins/testutils"
	"github.com/odpf/meteor/proto/odpf/meta"
	logger "github.com/odpf/salt/log"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	kafkaLib "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

var (
	brokerHost = "localhost:9093"
)

func TestMain(m *testing.M) {
	var conn *kafkaLib.Conn
	var broker kafkaLib.Broker

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
		conn, err = kafkaLib.Dial("tcp", brokerHost)
		if err != nil {
			return
		}
		broker, err = conn.Controller()
		if err != nil {
			conn.Close()
			return
		}

		return
	}
	err, purgeContainer := testutils.CreateContainer(opts, retryFn)
	if err != nil {
		log.Fatal(err)
	}

	// setup and populate kafka for testing
	if err := setup(broker); err != nil {
		log.Fatal(err)
	}

	// run tests
	code := m.Run()

	conn.Close()

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

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})

	t.Run("should return list of topic metadata", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		out := make(chan interface{})

		go func() {
			err := newExtractor().Extract(ctx, map[string]interface{}{
				"broker": brokerHost,
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

func setup(broker kafkaLib.Broker) (err error) {
	// create broker connection to create topics
	var conn *kafkaLib.Conn
	conn, err = kafkaLib.Dial("tcp", net.JoinHostPort(broker.Host, strconv.Itoa(broker.Port)))
	if err != nil {
		return
	}
	defer conn.Close()

	// create topics
	topicConfigs := []kafkaLib.TopicConfig{
		{Topic: "meteor-test-topic-1", NumPartitions: 1, ReplicationFactor: 1},
		{Topic: "meteor-test-topic-2", NumPartitions: 1, ReplicationFactor: 1},
		{Topic: "meteor-test-topic-3", NumPartitions: 1, ReplicationFactor: 1},
	}
	err = conn.CreateTopics(topicConfigs...)
	if err != nil {
		return
	}

	return
}

func newExtractor() *kafka.Extractor {
	return kafka.New(
		logger.NewLogrus(logger.LogrusWithWriter(ioutil.Discard)),
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
