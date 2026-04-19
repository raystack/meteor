//go:build plugins
// +build plugins

package dbt_test

import (
	"context"
	"testing"

	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	extractor "github.com/raystack/meteor/plugins/extractors/dbt"
	"github.com/raystack/meteor/test/mocks"
	testutils "github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const urnScope = "test-dbt"

func TestInit(t *testing.T) {
	t.Run("should return error when manifest is missing", func(t *testing.T) {
		err := extractor.New(testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope:  urnScope,
			RawConfig: map[string]any{},
		})
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should return error when manifest file does not exist", func(t *testing.T) {
		err := extractor.New(testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"manifest": "testdata/nonexistent.json",
			},
		})
		require.Error(t, err)
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should succeed with valid manifest", func(t *testing.T) {
		err := extractor.New(testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"manifest": "testdata/manifest.json",
			},
		})
		assert.NoError(t, err)
	})
}

func TestExtract(t *testing.T) {
	t.Run("should extract models with properties and lineage edges", func(t *testing.T) {
		extr := initExtractor(t, map[string]any{
			"manifest": "testdata/manifest.json",
		})

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		require.NoError(t, err)

		records := emitter.Get()
		// 2 models + 2 sources = 4 records (test nodes are skipped).
		require.Len(t, records, 4)

		// Find the customers model.
		customersRecord := findRecord(records, "model.jaffle_shop.customers")
		require.NotNil(t, customersRecord)

		entity := customersRecord.Entity()
		assert.Equal(t, "model", entity.GetType())
		assert.Equal(t, "customers", entity.GetName())
		assert.Equal(t, "dbt", entity.GetSource())
		assert.Equal(t, "This table has basic information about customers.", entity.GetDescription())

		props := entity.GetProperties().AsMap()
		assert.Equal(t, "analytics", props["database"])
		assert.Equal(t, "jaffle_shop", props["schema"])
		assert.Equal(t, "table", props["materialization"])
		assert.Equal(t, "models/customers.sql", props["sql_path"])
		assert.Equal(t, "sql", props["language"])

		// Should have 2 derived_from edges + 1 owned_by edge.
		edges := customersRecord.Edges()
		require.Len(t, edges, 3)

		derivedEdges := filterEdges(edges, "derived_from")
		assert.Len(t, derivedEdges, 2)

		ownerEdges := filterEdges(edges, "owned_by")
		require.Len(t, ownerEdges, 1)
		assert.Contains(t, ownerEdges[0].GetTargetUrn(), "analytics-team")
	})

	t.Run("should extract sources with properties and ownership", func(t *testing.T) {
		extr := initExtractor(t, map[string]any{
			"manifest": "testdata/manifest.json",
		})

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		require.NoError(t, err)

		records := emitter.Get()
		customersSource := findRecord(records, "source.jaffle_shop.raw.customers")
		require.NotNil(t, customersSource)

		entity := customersSource.Entity()
		assert.Equal(t, "source", entity.GetType())
		assert.Equal(t, "customers", entity.GetName())
		assert.Equal(t, "Raw customers from the payment system.", entity.GetDescription())

		props := entity.GetProperties().AsMap()
		assert.Equal(t, "raw_db", props["database"])
		assert.Equal(t, "raw_data", props["schema"])
		assert.Equal(t, "raw", props["source_name"])
		assert.Equal(t, "stitch", props["loader"])

		// Should have 1 owned_by edge.
		edges := customersSource.Edges()
		require.Len(t, edges, 1)
		assert.Equal(t, "owned_by", edges[0].GetType())
		assert.Contains(t, edges[0].GetTargetUrn(), "data-eng")
	})

	t.Run("should skip test nodes", func(t *testing.T) {
		extr := initExtractor(t, map[string]any{
			"manifest": "testdata/manifest.json",
		})

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		require.NoError(t, err)

		for _, r := range emitter.Get() {
			assert.NotEqual(t, "test", r.Entity().GetType())
		}
	})

	t.Run("should enrich columns with catalog data", func(t *testing.T) {
		extr := initExtractor(t, map[string]any{
			"manifest": "testdata/manifest.json",
			"catalog":  "testdata/catalog.json",
		})

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		require.NoError(t, err)

		records := emitter.Get()
		customersRecord := findRecord(records, "model.jaffle_shop.customers")
		require.NotNil(t, customersRecord)

		props := customersRecord.Entity().GetProperties().AsMap()
		columns, ok := props["columns"].([]any)
		require.True(t, ok)
		require.Len(t, columns, 2)

		// Find the "id" column — manifest has no data_type, catalog has INTEGER.
		idCol := findColumn(columns, "id")
		require.NotNil(t, idCol)
		assert.Equal(t, "INTEGER", idCol["data_type"])
	})

	t.Run("should not emit ownership edge when meta.owner is absent", func(t *testing.T) {
		extr := initExtractor(t, map[string]any{
			"manifest": "testdata/manifest.json",
		})

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		require.NoError(t, err)

		// stg_orders has empty meta, so no owned_by edge.
		records := emitter.Get()
		stgOrders := findRecord(records, "model.jaffle_shop.stg_orders")
		require.NotNil(t, stgOrders)

		for _, edge := range stgOrders.Edges() {
			assert.NotEqual(t, "owned_by", edge.GetType())
		}
	})
}

// --- helpers ---

func initExtractor(t *testing.T, rawConfig map[string]any) *extractor.Extractor {
	t.Helper()
	extr := extractor.New(testutils.Logger)
	err := extr.Init(context.Background(), plugins.Config{
		URNScope:  urnScope,
		RawConfig: rawConfig,
	})
	require.NoError(t, err)
	return extr
}

func findRecord(records []models.Record, uniqueID string) *models.Record {
	suffix := uniqueID
	for i, r := range records {
		if urn := r.Entity().GetUrn(); len(urn) > 0 && contains(urn, suffix) {
			return &records[i]
		}
	}
	return nil
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchSubstring(s, substr)
}

func searchSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func filterEdges(edges []*meteorv1beta1.Edge, typ string) []*meteorv1beta1.Edge {
	var out []*meteorv1beta1.Edge
	for _, e := range edges {
		if e.GetType() == typ {
			out = append(out, e)
		}
	}
	return out
}

func findColumn(columns []any, name string) map[string]any {
	for _, c := range columns {
		col, ok := c.(map[string]any)
		if ok && col["name"] == name {
			return col
		}
	}
	return nil
}
