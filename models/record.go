package models

import (
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
)

// Record is the unit of metadata that flows through Meteor's pipeline.
// An extractor emits one Record per discovered resource.
type Record struct {
	entity *meteorv1beta1.Entity
	edges  []*meteorv1beta1.Edge
}

// NewRecord creates a new record with an entity and optional edges.
func NewRecord(entity *meteorv1beta1.Entity, edges ...*meteorv1beta1.Edge) Record {
	return Record{entity: entity, edges: edges}
}

// Entity returns the entity in this record.
func (r Record) Entity() *meteorv1beta1.Entity {
	return r.entity
}

// Edges returns the edges in this record.
func (r Record) Edges() []*meteorv1beta1.Edge {
	return r.edges
}
