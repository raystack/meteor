package models_test

import (
	"fmt"
	"testing"

	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewURN(t *testing.T) {
	testCases := []struct {
		service  string
		scope    string
		kind     string
		id       string
		expected string
	}{
		{
			"metabase", "main-dashboard", "collection", "123",
			"urn:metabase:main-dashboard:collection:123",
		},
		{
			"bigquery", "p-godata-id", "table", "p-godata-id:mydataset.mytable",
			"urn:bigquery:p-godata-id:table:p-godata-id:mydataset.mytable",
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("should return expected urn (#%d)", i+1), func(t *testing.T) {
			actual := models.NewURN(tc.service, tc.scope, tc.kind, tc.id)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestNewEntity(t *testing.T) {
	entity := models.NewEntity("urn:test:s:table:t1", "table", "t1", "test", map[string]any{
		"columns": []any{"id", "name"},
	})
	assert.Equal(t, "urn:test:s:table:t1", entity.GetUrn())
	assert.Equal(t, "table", entity.GetType())
	assert.Equal(t, "t1", entity.GetName())
	assert.Equal(t, "test", entity.GetSource())
	assert.NotNil(t, entity.GetProperties())
}

func TestDerivedFromEdge(t *testing.T) {
	edge := models.DerivedFromEdge("urn:view", "urn:table", "bigquery")
	assert.Equal(t, "urn:view", edge.GetSourceUrn())
	assert.Equal(t, "urn:table", edge.GetTargetUrn())
	assert.Equal(t, "derived_from", edge.GetType())
	assert.Equal(t, "bigquery", edge.GetSource())
}

func TestGeneratesEdge(t *testing.T) {
	edge := models.GeneratesEdge("urn:job", "urn:table", "optimus")
	assert.Equal(t, "urn:job", edge.GetSourceUrn())
	assert.Equal(t, "urn:table", edge.GetTargetUrn())
	assert.Equal(t, "generates", edge.GetType())
	assert.Equal(t, "optimus", edge.GetSource())
}

func TestOwnerEdge(t *testing.T) {
	edge := models.OwnerEdge("urn:table", "urn:user:alice@co.com", "bigquery")
	assert.Equal(t, "urn:table", edge.GetSourceUrn())
	assert.Equal(t, "urn:user:alice@co.com", edge.GetTargetUrn())
	assert.Equal(t, "owned_by", edge.GetType())
}

func TestEntityToJSON(t *testing.T) {
	entity := &meteorv1beta1.Entity{
		Urn:    "urn:test:s:table:t1",
		Name:   "t1",
		Type:   "table",
		Source: "test",
	}
	b, err := models.EntityToJSON(entity)
	require.NoError(t, err)
	assert.Contains(t, string(b), `"urn":"urn:test:s:table:t1"`)
	assert.Contains(t, string(b), `"name":"t1"`)
}

func TestRecordToJSON(t *testing.T) {
	entity := &meteorv1beta1.Entity{
		Urn:  "urn:test:s:table:t1",
		Name: "t1",
	}
	edge := models.DerivedFromEdge("urn:a", "urn:b", "test")
	record := models.NewRecord(entity, edge)

	b, err := models.RecordToJSON(record)
	require.NoError(t, err)
	assert.Contains(t, string(b), `"entity"`)
	assert.Contains(t, string(b), `"edges"`)
}

func TestNewEntityWithNilProps(t *testing.T) {
	entity := models.NewEntity("urn:test:s:table:t1", "table", "t1", "test", nil)
	assert.Equal(t, "urn:test:s:table:t1", entity.GetUrn())
	assert.Equal(t, "table", entity.GetType())
	assert.Nil(t, entity.GetProperties())
}

func TestNewEntityWithEmptyProps(t *testing.T) {
	entity := models.NewEntity("urn:test:s:table:t1", "table", "t1", "test", map[string]any{})
	assert.Equal(t, "urn:test:s:table:t1", entity.GetUrn())
	assert.Nil(t, entity.GetProperties())
}

func TestNewEntityWithNestedProps(t *testing.T) {
	entity := models.NewEntity("urn:test:s:table:t1", "table", "t1", "test", map[string]any{
		"labels": map[string]string{"env": "production", "team": "data"},
		"tags":   []string{"important", "verified"},
	})
	assert.Equal(t, "urn:test:s:table:t1", entity.GetUrn())
	props := entity.GetProperties()
	require.NotNil(t, props)
	labelsVal := props.GetFields()["labels"].GetStructValue()
	require.NotNil(t, labelsVal)
	assert.Equal(t, "production", labelsVal.GetFields()["env"].GetStringValue())
	assert.Equal(t, "data", labelsVal.GetFields()["team"].GetStringValue())
	tagsVal := props.GetFields()["tags"].GetListValue()
	require.NotNil(t, tagsVal)
	assert.Len(t, tagsVal.GetValues(), 2)
	assert.Equal(t, "important", tagsVal.GetValues()[0].GetStringValue())
}

func TestRecordToJSONWithoutEdges(t *testing.T) {
	entity := &meteorv1beta1.Entity{
		Urn:  "urn:test:s:table:t1",
		Name: "t1",
	}
	record := models.NewRecord(entity)

	b, err := models.RecordToJSON(record)
	require.NoError(t, err)
	assert.Contains(t, string(b), `"entity"`)
	assert.NotContains(t, string(b), `"edges"`)
}

func TestRecordToJSONWithMultipleEdges(t *testing.T) {
	entity := &meteorv1beta1.Entity{
		Urn:  "urn:test:s:table:t1",
		Name: "t1",
	}
	derived := models.DerivedFromEdge("urn:test:s:table:t1", "urn:b", "test")
	owner := models.OwnerEdge("urn:test:s:table:t1", "urn:user:bob@co.com", "test")
	record := models.NewRecord(entity, derived, owner)

	b, err := models.RecordToJSON(record)
	require.NoError(t, err)
	s := string(b)
	assert.Contains(t, s, `"entity"`)
	assert.Contains(t, s, `"edges"`)
	assert.Contains(t, s, `"derived_from"`)
	assert.Contains(t, s, `"owned_by"`)
	assert.Contains(t, s, `"urn:user:bob@co.com"`)
}

func TestRecordToMarkdown(t *testing.T) {
	t.Run("minimal entity without properties or edges", func(t *testing.T) {
		entity := models.NewEntity("urn:test:s:table:t1", "table", "t1", "test", nil)
		record := models.NewRecord(entity)

		md, err := models.RecordToMarkdown(record)
		require.NoError(t, err)
		s := string(md)
		assert.Contains(t, s, "## t1")
		assert.Contains(t, s, "| URN | `urn:test:s:table:t1` |")
		assert.Contains(t, s, "| Type | table |")
		assert.Contains(t, s, "| Source | test |")
		assert.NotContains(t, s, "### Properties")
		assert.NotContains(t, s, "### Edges")
	})

	t.Run("entity with flat properties", func(t *testing.T) {
		entity := models.NewEntity("urn:test:s:table:t1", "table", "t1", "test", map[string]any{
			"database": "analytics",
			"schema":   "public",
		})
		record := models.NewRecord(entity)

		md, err := models.RecordToMarkdown(record)
		require.NoError(t, err)
		s := string(md)
		assert.Contains(t, s, "### Properties")
		assert.Contains(t, s, "- **database**: analytics")
		assert.Contains(t, s, "- **schema**: public")
	})

	t.Run("entity with list-of-maps properties rendered as table", func(t *testing.T) {
		entity := models.NewEntity("urn:test:s:table:t1", "table", "t1", "test", map[string]any{
			"columns": []any{
				map[string]any{"name": "id", "data_type": "integer"},
				map[string]any{"name": "email", "data_type": "varchar"},
			},
		})
		record := models.NewRecord(entity)

		md, err := models.RecordToMarkdown(record)
		require.NoError(t, err)
		s := string(md)
		assert.Contains(t, s, "### Columns")
		assert.Contains(t, s, "| Data Type | Name |")
		assert.Contains(t, s, "| integer | id |")
		assert.Contains(t, s, "| varchar | email |")
	})

	t.Run("entity with edges", func(t *testing.T) {
		entity := models.NewEntity("urn:test:s:table:t1", "table", "t1", "test", nil)
		edges := []*meteorv1beta1.Edge{
			models.OwnerEdge("urn:test:s:table:t1", "urn:user:alice", "test"),
			models.DerivedFromEdge("urn:test:s:table:t1", "urn:test:s:table:upstream", "test"),
		}
		record := models.NewRecord(entity, edges...)

		md, err := models.RecordToMarkdown(record)
		require.NoError(t, err)
		s := string(md)
		assert.Contains(t, s, "### Edges")
		assert.Contains(t, s, "| owned_by | `urn:test:s:table:t1` | `urn:user:alice` |")
		assert.Contains(t, s, "| derived_from | `urn:test:s:table:t1` | `urn:test:s:table:upstream` |")
	})

	t.Run("entity with description", func(t *testing.T) {
		entity := &meteorv1beta1.Entity{
			Urn:         "urn:test:s:table:t1",
			Name:        "t1",
			Type:        "table",
			Source:      "test",
			Description: "Event tracking table",
		}
		record := models.NewRecord(entity)

		md, err := models.RecordToMarkdown(record)
		require.NoError(t, err)
		assert.Contains(t, string(md), "| Description | Event tracking table |")
	})

	t.Run("entity with nested map properties", func(t *testing.T) {
		entity := models.NewEntity("urn:test:s:table:t1", "table", "t1", "test", map[string]any{
			"labels": map[string]string{"env": "production", "team": "data"},
		})
		record := models.NewRecord(entity)

		md, err := models.RecordToMarkdown(record)
		require.NoError(t, err)
		s := string(md)
		assert.Contains(t, s, "- **labels**:")
		assert.Contains(t, s, "  - **env**: production")
		assert.Contains(t, s, "  - **team**: data")
	})

	t.Run("entity with scalar list properties", func(t *testing.T) {
		entity := models.NewEntity("urn:test:s:table:t1", "table", "t1", "test", map[string]any{
			"tags": []string{"important", "verified"},
		})
		record := models.NewRecord(entity)

		md, err := models.RecordToMarkdown(record)
		require.NoError(t, err)
		assert.Contains(t, string(md), "- **tags**: important, verified")
	})
}
