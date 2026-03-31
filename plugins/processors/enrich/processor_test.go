//go:build plugins

package enrich_test

import (
	"context"
	"testing"

	"github.com/raystack/meteor/models"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/processors/enrich"
	testutils "github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInit(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		proc := enrich.New(testutils.Logger)
		err := proc.Init(context.Background(), plugins.Config{
			RawConfig: map[string]any{},
		})
		assert.Error(t, err)
	})

	t.Run("should return no error for valid config", func(t *testing.T) {
		proc := enrich.New(testutils.Logger)
		err := proc.Init(context.Background(), plugins.Config{
			RawConfig: map[string]any{
				"attributes": map[string]any{
					"team": "data-engineering",
				},
			},
		})
		assert.NoError(t, err)
	})
}

func TestProcess(t *testing.T) {
	t.Run("should enrich entity with nil properties", func(t *testing.T) {
		proc := enrich.New(testutils.Logger)
		err := proc.Init(context.Background(), plugins.Config{
			RawConfig: map[string]any{
				"attributes": map[string]any{
					"team": "data-engineering",
				},
			},
		})
		require.NoError(t, err)

		entity := models.NewEntity("urn:table:1", "table", "my-table", "bigquery", nil)
		rec := models.NewRecord(entity)

		result, err := proc.Process(context.Background(), rec)
		require.NoError(t, err)

		props := result.Entity().GetProperties().AsMap()
		assert.Equal(t, "data-engineering", props["team"])
	})

	t.Run("should merge with existing properties", func(t *testing.T) {
		proc := enrich.New(testutils.Logger)
		err := proc.Init(context.Background(), plugins.Config{
			RawConfig: map[string]any{
				"attributes": map[string]any{
					"team": "data-engineering",
				},
			},
		})
		require.NoError(t, err)

		entity := models.NewEntity("urn:table:1", "table", "my-table", "bigquery", map[string]any{
			"existing_key": "existing_value",
		})
		rec := models.NewRecord(entity)

		result, err := proc.Process(context.Background(), rec)
		require.NoError(t, err)

		props := result.Entity().GetProperties().AsMap()
		assert.Equal(t, "existing_value", props["existing_key"])
		assert.Equal(t, "data-engineering", props["team"])
	})

	t.Run("should preserve edges through processing", func(t *testing.T) {
		proc := enrich.New(testutils.Logger)
		err := proc.Init(context.Background(), plugins.Config{
			RawConfig: map[string]any{
				"attributes": map[string]any{
					"team": "data-engineering",
				},
			},
		})
		require.NoError(t, err)

		entity := models.NewEntity("urn:table:1", "table", "my-table", "bigquery", nil)
		lineage := models.LineageEdge("urn:table:1", "urn:table:2", "bigquery")
		owner := models.OwnerEdge("urn:table:1", "urn:user:alice", "bigquery")
		rec := models.NewRecord(entity, lineage, owner)

		result, err := proc.Process(context.Background(), rec)
		require.NoError(t, err)

		assert.Len(t, result.Edges(), 2)
		assert.Equal(t, lineage, result.Edges()[0])
		assert.Equal(t, owner, result.Edges()[1])
	})

	t.Run("should only add string values and skip non-string values", func(t *testing.T) {
		proc := enrich.New(testutils.Logger)
		err := proc.Init(context.Background(), plugins.Config{
			RawConfig: map[string]any{
				"attributes": map[string]any{
					"team":     "data-engineering",
					"count":    42,
					"enabled":  true,
					"fraction": 3.14,
				},
			},
		})
		require.NoError(t, err)

		entity := models.NewEntity("urn:table:1", "table", "my-table", "bigquery", nil)
		rec := models.NewRecord(entity)

		result, err := proc.Process(context.Background(), rec)
		require.NoError(t, err)

		props := result.Entity().GetProperties().AsMap()
		assert.Equal(t, "data-engineering", props["team"])
		assert.Nil(t, props["count"])
		assert.Nil(t, props["enabled"])
		assert.Nil(t, props["fraction"])
	})
}
