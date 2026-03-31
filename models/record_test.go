package models_test

import (
	"testing"

	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/stretchr/testify/assert"
)

func TestNewRecord(t *testing.T) {
	entity := &meteorv1beta1.Entity{
		Urn:  "urn:test:scope:table:t1",
		Name: "t1",
	}
	record := models.NewRecord(entity)
	assert.Equal(t, entity, record.Entity())
	assert.Empty(t, record.Edges())
}

func TestNewRecordWithEdges(t *testing.T) {
	entity := &meteorv1beta1.Entity{
		Urn:  "urn:test:scope:table:t1",
		Name: "t1",
	}
	edge := &meteorv1beta1.Edge{
		SourceUrn: "urn:test:scope:table:t1",
		TargetUrn: "urn:user:alice@co.com",
		Type:      "owned_by",
		Source:    "test",
	}
	record := models.NewRecord(entity, edge)
	assert.Equal(t, entity, record.Entity())
	assert.Len(t, record.Edges(), 1)
	assert.Equal(t, edge, record.Edges()[0])
}

func TestNewRecordWithMultipleEdges(t *testing.T) {
	entity := &meteorv1beta1.Entity{
		Urn:  "urn:test:scope:table:t1",
		Name: "t1",
		Type: "table",
	}
	lineageEdge := &meteorv1beta1.Edge{
		SourceUrn: "urn:test:scope:table:t1",
		TargetUrn: "urn:test:scope:table:t2",
		Type:      "lineage",
		Source:    "test",
	}
	ownerEdge := &meteorv1beta1.Edge{
		SourceUrn: "urn:test:scope:table:t1",
		TargetUrn: "urn:user:alice@co.com",
		Type:      "owned_by",
		Source:    "test",
	}
	record := models.NewRecord(entity, lineageEdge, ownerEdge)
	assert.Equal(t, entity, record.Entity())
	assert.Len(t, record.Edges(), 2)
	assert.Equal(t, lineageEdge, record.Edges()[0])
	assert.Equal(t, ownerEdge, record.Edges()[1])
	assert.Equal(t, "lineage", record.Edges()[0].GetType())
	assert.Equal(t, "owned_by", record.Edges()[1].GetType())
}

func TestNewRecordWithNilEntity(t *testing.T) {
	record := models.NewRecord(nil)
	assert.Nil(t, record.Entity())
	assert.Empty(t, record.Edges())
}
