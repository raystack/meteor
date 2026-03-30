package applicationyaml

import (
	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
)

const (
	service = "application_yaml"
	typ     = "application"
)

func buildRecord(scope string, svc Application) models.Record {
	urn := models.NewURN(service, scope, typ, svc.Name)

	props := map[string]any{}
	if svc.ID != "" {
		props["id"] = svc.ID
	}
	if svc.Version != "" {
		props["version"] = svc.Version
	}
	if svc.URL != "" {
		props["url"] = svc.URL
	}
	if !svc.CreateTime.IsZero() {
		props["create_time"] = svc.CreateTime.Format("2006-01-02T15:04:05Z")
	}
	if !svc.UpdateTime.IsZero() {
		props["update_time"] = svc.UpdateTime.Format("2006-01-02T15:04:05Z")
	}
	if len(svc.Labels) > 0 {
		props["labels"] = svc.Labels
	}

	// Build edges
	var edges []*meteorv1beta1.Edge

	// Owner edge
	if svc.Team.ID != "" {
		ownerURN := "urn:user:" + svc.Team.Email
		if svc.Team.Email == "" {
			ownerURN = "urn:user:" + svc.Team.ID
		}
		edges = append(edges, models.OwnerEdge(urn, ownerURN, service))
	}

	// Lineage edges: upstreams (inputs -> this entity)
	for _, inputURN := range svc.Inputs {
		edges = append(edges, models.LineageEdge(inputURN, urn, service))
	}
	// Lineage edges: downstreams (this entity -> outputs)
	for _, outputURN := range svc.Outputs {
		edges = append(edges, models.LineageEdge(urn, outputURN, service))
	}

	entity := models.NewEntity(urn, typ, svc.Name, service, props)
	if svc.Description != "" {
		entity.Description = svc.Description
	}

	return models.NewRecord(entity, edges...)
}
