//go:build plugins

package console_test

import (
	"context"
	"testing"

	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/sinks/console"
	testutils "github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInit(t *testing.T) {
	t.Run("should return no error for valid config", func(t *testing.T) {
		sink := console.New(testutils.Logger)
		err := sink.Init(context.Background(), plugins.Config{RawConfig: map[string]interface{}{}})
		assert.NoError(t, err)
	})

	t.Run("should accept json format", func(t *testing.T) {
		sink := console.New(testutils.Logger)
		err := sink.Init(context.Background(), plugins.Config{RawConfig: map[string]interface{}{
			"format": "json",
		}})
		assert.NoError(t, err)
	})

	t.Run("should accept markdown format", func(t *testing.T) {
		sink := console.New(testutils.Logger)
		err := sink.Init(context.Background(), plugins.Config{RawConfig: map[string]interface{}{
			"format": "markdown",
		}})
		assert.NoError(t, err)
	})

	t.Run("should return error for invalid format", func(t *testing.T) {
		sink := console.New(testutils.Logger)
		err := sink.Init(context.Background(), plugins.Config{RawConfig: map[string]interface{}{
			"format": "xml",
		}})
		assert.Error(t, err)
	})
}

func TestSink(t *testing.T) {
	t.Run("should sink single record without error", func(t *testing.T) {
		sink := console.New(testutils.Logger)
		require.NoError(t, sink.Init(context.Background(), plugins.Config{RawConfig: map[string]interface{}{}}))

		entity := models.NewEntity("urn:test:scope:table:myid", "table", "my-table", "test", nil)
		record := models.NewRecord(entity)

		err := sink.Sink(context.Background(), []models.Record{record})
		assert.NoError(t, err)
	})

	t.Run("should sink multiple records without error", func(t *testing.T) {
		sink := console.New(testutils.Logger)
		require.NoError(t, sink.Init(context.Background(), plugins.Config{RawConfig: map[string]interface{}{}}))

		records := []models.Record{
			models.NewRecord(models.NewEntity("urn:test:scope:table:t1", "table", "table-1", "test", nil)),
			models.NewRecord(models.NewEntity("urn:test:scope:topic:t2", "topic", "topic-1", "kafka", map[string]any{
				"partitions": 3,
			})),
		}

		err := sink.Sink(context.Background(), records)
		assert.NoError(t, err)
	})

	t.Run("should handle empty batch", func(t *testing.T) {
		sink := console.New(testutils.Logger)
		require.NoError(t, sink.Init(context.Background(), plugins.Config{RawConfig: map[string]interface{}{}}))

		err := sink.Sink(context.Background(), []models.Record{})
		assert.NoError(t, err)
	})

	t.Run("should sink record with edges", func(t *testing.T) {
		sink := console.New(testutils.Logger)
		require.NoError(t, sink.Init(context.Background(), plugins.Config{RawConfig: map[string]interface{}{}}))

		entity := models.NewEntity("urn:test:scope:table:t1", "table", "my-table", "test", map[string]any{
			"database": "testdb",
		})
		edges := []*meteorv1beta1.Edge{
			models.OwnerEdge("urn:test:scope:table:t1", "urn:test:scope:user:alice", "test"),
			models.DerivedFromEdge("urn:test:scope:table:t1", "urn:test:scope:table:upstream", "test"),
		}
		record := models.NewRecord(entity, edges...)

		err := sink.Sink(context.Background(), []models.Record{record})
		assert.NoError(t, err)
	})

	t.Run("should sink markdown format without error", func(t *testing.T) {
		sink := console.New(testutils.Logger)
		require.NoError(t, sink.Init(context.Background(), plugins.Config{RawConfig: map[string]interface{}{
			"format": "markdown",
		}}))

		entity := models.NewEntity("urn:test:scope:table:t1", "table", "my-table", "test", map[string]any{
			"database": "testdb",
			"columns": []any{
				map[string]any{"name": "id", "data_type": "integer"},
			},
		})
		edges := []*meteorv1beta1.Edge{
			models.OwnerEdge("urn:test:scope:table:t1", "urn:test:scope:user:alice", "test"),
		}
		record := models.NewRecord(entity, edges...)

		err := sink.Sink(context.Background(), []models.Record{record})
		assert.NoError(t, err)
	})
}

func TestClose(t *testing.T) {
	t.Run("should return nil on close", func(t *testing.T) {
		sink := console.New(testutils.Logger)
		require.NoError(t, sink.Init(context.Background(), plugins.Config{RawConfig: map[string]interface{}{}}))

		err := sink.Close()
		assert.NoError(t, err)
	})
}
