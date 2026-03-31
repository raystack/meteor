//go:build plugins
// +build plugins

package kafka_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/sinks/kafka"
	testutils "github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInit(t *testing.T) {
	t.Run("should return InvalidConfigError on invalid config", func(t *testing.T) {
		invalidConfigs := []map[string]any{
			{},
			{"brokers": "localhost:9092"},
			{"topic": "test-topic"},
			{"brokers": "", "topic": ""},
		}
		for i, config := range invalidConfigs {
			t.Run(fmt.Sprintf("test invalid config #%d", i+1), func(t *testing.T) {
				sink := kafka.New(testutils.Logger)
				err := sink.Init(context.TODO(), plugins.Config{RawConfig: config})

				assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
			})
		}
	})

	t.Run("should not return error on valid config", func(t *testing.T) {
		sink := kafka.New(testutils.Logger)
		err := sink.Init(context.TODO(), plugins.Config{RawConfig: map[string]any{
			"brokers": "localhost:9092",
			"topic":   "test-topic",
		}})

		require.NoError(t, err)

		// Clean up the writer created during Init.
		err = sink.Close()
		assert.NoError(t, err)
	})

	t.Run("should not return error on valid config with key_path", func(t *testing.T) {
		sink := kafka.New(testutils.Logger)
		err := sink.Init(context.TODO(), plugins.Config{RawConfig: map[string]any{
			"brokers":  "localhost:9092",
			"topic":    "test-topic",
			"key_path": ".Urn",
		}})

		require.NoError(t, err)

		err = sink.Close()
		assert.NoError(t, err)
	})
}
