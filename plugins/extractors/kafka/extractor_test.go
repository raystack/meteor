//+build integration

package kafka_test

import (
	"io/ioutil"
	"testing"

	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/logger"
	"github.com/odpf/meteor/plugins/extractors/kafka"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/stretchr/testify/assert"
)

var log = logger.NewWithWriter("info", ioutil.Discard)

func TestExtractorExtract(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		extr := kafka.New(log)
		_, err := extr.Extract(map[string]interface{}{
			"wrong-config": "wrong-value",
		})

		assert.Equal(t, extractor.InvalidConfigError{}, err)
	})

	t.Run("should return list of topic metadata", func(t *testing.T) {
		extractor := kafka.New(log)
		result, err := extractor.Extract(map[string]interface{}{
			"broker": "localhost:9092",
		})
		if err != nil {
			t.Fatal(err)
		}

		expected := []meta.Topic{
			{
				Urn:    "my-topic-1",
				Name:   "my-topic-1",
				Source: "kafka",
			},
			{
				Urn:    "my-topic-2",
				Name:   "my-topic-2",
				Source: "kafka",
			},
			{
				Urn:    "my-topic-3",
				Name:   "my-topic-3",
				Source: "kafka",
			},
		}

		// We need this function because the extractor cannot guarantee order
		// so comparing expected slice and result slice will not be consistant
		assertResults(t, expected, result)
	})
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
