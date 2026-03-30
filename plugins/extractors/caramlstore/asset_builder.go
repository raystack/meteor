package caramlstore

import (
	"errors"
	"fmt"

	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/extractors/caramlstore/internal/core"
)

const (
	service = "caramlstore"
	typ     = "feature_table"
)

type featureTableBuilder struct {
	scope    string
	project  string
	entities map[string]*core.Entity
}

func (b featureTableBuilder) buildRecord(ft *core.FeatureTable) (models.Record, error) {
	fail := func(step string, err error) (models.Record, error) {
		return models.Record{}, fmt.Errorf(
			"build %s for feature table '%s' in project '%s': %w",
			step, ft.Spec.Name, b.project, err,
		)
	}

	upstreamURNs, err := b.buildUpstreamURNs(ft)
	if err != nil {
		return fail("lineage", err)
	}

	entities, err := b.buildEntities(ft)
	if err != nil {
		return fail("entities", err)
	}

	urn := plugins.CaraMLStoreURN(b.scope, b.project, ft.Spec.Name)

	// Build edges
	var edges []*meteorv1beta1.Edge
	for _, upstreamURN := range upstreamURNs {
		edges = append(edges, models.LineageEdge(upstreamURN, urn, service))
	}

	props := map[string]interface{}{
		"namespace": b.project,
	}
	if len(entities) > 0 {
		props["entities"] = entities
	}
	features := b.buildFeatures(ft)
	if len(features) > 0 {
		props["features"] = features
	}
	if ft.Meta.CreatedTimestamp != nil {
		props["create_time"] = ft.Meta.CreatedTimestamp.AsTime().Format("2006-01-02T15:04:05Z")
	}
	if ft.Meta.LastUpdatedTimestamp != nil {
		props["update_time"] = ft.Meta.LastUpdatedTimestamp.AsTime().Format("2006-01-02T15:04:05Z")
	}
	if len(ft.Spec.Labels) > 0 {
		props["labels"] = ft.Spec.Labels
	}

	entity := models.NewEntity(urn, typ, ft.Spec.Name, service, props)
	return models.NewRecord(entity, edges...), nil
}

func (b featureTableBuilder) buildEntities(ft *core.FeatureTable) ([]map[string]interface{}, error) {
	entities := make([]map[string]interface{}, 0, len(ft.Spec.Entities))
	for _, e := range ft.Spec.Entities {
		entity, ok := b.entities[e]
		if !ok {
			return nil, fmt.Errorf("entity '%s' not found in project '%s", e, b.project)
		}

		m := map[string]interface{}{
			"name":        entity.Spec.Name,
			"description": entity.Spec.Description,
			"type":        entity.Spec.ValueType.String(),
		}
		if len(entity.Spec.Labels) > 0 {
			m["labels"] = entity.Spec.Labels
		}

		entities = append(entities, m)
	}

	return entities, nil
}

func (b featureTableBuilder) buildFeatures(ft *core.FeatureTable) []map[string]interface{} {
	features := make([]map[string]interface{}, 0, len(ft.Spec.Features))
	for _, f := range ft.Spec.Features {
		features = append(features, map[string]interface{}{
			"name":      f.Name,
			"data_type": f.ValueType.String(),
		})
	}

	return features
}

func (b featureTableBuilder) buildUpstreamURNs(ft *core.FeatureTable) ([]string, error) {
	var urns []string
	if src := ft.Spec.BatchSource; src != nil {
		switch src.Type {
		case core.DataSource_BATCH_BIGQUERY:
			opts := src.GetBigqueryOptions()
			if opts == nil {
				return nil, errors.New("build upstream: empty big query data source options")
			}

			urn, err := plugins.BigQueryTableFQNToURN(opts.TableRef)
			if err != nil {
				return nil, fmt.Errorf("build upstream: table ref: %s: %w", opts.TableRef, err)
			}

			urns = append(urns, urn)
		}
	}
	if src := ft.Spec.StreamSource; src != nil {
		switch src.Type {
		case core.DataSource_STREAM_KAFKA:
			opts := src.GetKafkaOptions()
			if opts == nil {
				return nil, errors.New("build upstream: empty kafka data source options")
			}

			urns = append(urns, plugins.KafkaURN(opts.BootstrapServers, opts.Topic))
		}
	}

	return urns, nil
}
