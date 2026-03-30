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
	entity := models.NewEntity("urn:test:s:table:t1", "table", "t1", "test", map[string]interface{}{
		"columns": []interface{}{"id", "name"},
	})
	assert.Equal(t, "urn:test:s:table:t1", entity.GetUrn())
	assert.Equal(t, "table", entity.GetType())
	assert.Equal(t, "t1", entity.GetName())
	assert.Equal(t, "test", entity.GetSource())
	assert.NotNil(t, entity.GetProperties())
}

func TestLineageEdge(t *testing.T) {
	edge := models.LineageEdge("urn:source", "urn:target", "bigquery")
	assert.Equal(t, "urn:source", edge.GetSourceUrn())
	assert.Equal(t, "urn:target", edge.GetTargetUrn())
	assert.Equal(t, "lineage", edge.GetType())
	assert.Equal(t, "bigquery", edge.GetSource())
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
	edge := models.LineageEdge("urn:a", "urn:b", "test")
	record := models.NewRecord(entity, edge)

	b, err := models.RecordToJSON(record)
	require.NoError(t, err)
	assert.Contains(t, string(b), `"entity"`)
	assert.Contains(t, string(b), `"edges"`)
}
