//go:build plugins

package labels_test

import (
	"context"
	"testing"

	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/processors/labels"
	testutils "github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInit(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		p := labels.New(testutils.Logger)
		err := p.Init(context.Background(), plugins.Config{
			RawConfig: map[string]any{},
		})
		assert.Error(t, err)
	})

	t.Run("should return no error for valid config", func(t *testing.T) {
		p := labels.New(testutils.Logger)
		err := p.Init(context.Background(), plugins.Config{
			RawConfig: map[string]any{
				"labels": map[string]any{
					"team": "data-eng",
				},
			},
		})
		assert.NoError(t, err)
	})
}

func TestProcess(t *testing.T) {
	ctx := context.Background()

	t.Run("should add labels to entity with nil properties", func(t *testing.T) {
		p := labels.New(testutils.Logger)
		require.NoError(t, p.Init(ctx, plugins.Config{
			RawConfig: map[string]any{
				"labels": map[string]any{
					"team": "data-eng",
				},
			},
		}))

		entity := models.NewEntity("urn:test:scope:table:t1", "table", "t1", "test", nil)
		rec := models.NewRecord(entity)

		result, err := p.Process(ctx, rec)
		require.NoError(t, err)

		props := result.Entity().GetProperties().AsMap()
		lbls, ok := props["labels"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "data-eng", lbls["team"])
	})

	t.Run("should add labels to entity with existing properties but no labels", func(t *testing.T) {
		p := labels.New(testutils.Logger)
		require.NoError(t, p.Init(ctx, plugins.Config{
			RawConfig: map[string]any{
				"labels": map[string]any{
					"env": "production",
				},
			},
		}))

		entity := models.NewEntity("urn:test:scope:table:t2", "table", "t2", "test", map[string]any{
			"description": "some table",
		})
		rec := models.NewRecord(entity)

		result, err := p.Process(ctx, rec)
		require.NoError(t, err)

		props := result.Entity().GetProperties().AsMap()
		assert.Equal(t, "some table", props["description"])
		lbls, ok := props["labels"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "production", lbls["env"])
	})

	t.Run("should merge labels with existing labels", func(t *testing.T) {
		p := labels.New(testutils.Logger)
		require.NoError(t, p.Init(ctx, plugins.Config{
			RawConfig: map[string]any{
				"labels": map[string]any{
					"env":  "production",
					"team": "data-eng",
				},
			},
		}))

		entity := models.NewEntity("urn:test:scope:table:t3", "table", "t3", "test", map[string]any{
			"labels": map[string]any{
				"existing": "value",
				"team":     "old-team",
			},
		})
		rec := models.NewRecord(entity)

		result, err := p.Process(ctx, rec)
		require.NoError(t, err)

		props := result.Entity().GetProperties().AsMap()
		lbls, ok := props["labels"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "value", lbls["existing"])
		assert.Equal(t, "data-eng", lbls["team"])
		assert.Equal(t, "production", lbls["env"])
	})

	t.Run("should preserve edges through processing", func(t *testing.T) {
		p := labels.New(testutils.Logger)
		require.NoError(t, p.Init(ctx, plugins.Config{
			RawConfig: map[string]any{
				"labels": map[string]any{
					"team": "data-eng",
				},
			},
		}))

		entity := models.NewEntity("urn:test:scope:table:t4", "table", "t4", "test", nil)
		edges := []*meteorv1beta1.Edge{
			models.OwnerEdge("urn:test:scope:table:t4", "urn:test:scope:user:alice", "test"),
			models.LineageEdge("urn:test:scope:table:t4", "urn:test:scope:table:t5", "test"),
		}
		rec := models.NewRecord(entity, edges...)

		result, err := p.Process(ctx, rec)
		require.NoError(t, err)

		resultEdges := result.Edges()
		require.Len(t, resultEdges, 2)
		assert.Equal(t, "owned_by", resultEdges[0].Type)
		assert.Equal(t, "urn:test:scope:user:alice", resultEdges[0].TargetUrn)
		assert.Equal(t, "lineage", resultEdges[1].Type)
		assert.Equal(t, "urn:test:scope:table:t5", resultEdges[1].TargetUrn)
	})
}
