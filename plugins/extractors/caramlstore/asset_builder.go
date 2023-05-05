package caramlstore

import (
	"errors"
	"fmt"

	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/extractors/caramlstore/internal/core"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
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

func (b featureTableBuilder) buildAsset(ft *core.FeatureTable) (*v1beta2.Asset, error) {
	fail := func(step string, err error) (*v1beta2.Asset, error) {
		return nil, fmt.Errorf(
			"build %s for feature table '%s' in project '%s': %w",
			step, ft.Spec.Name, b.project, err,
		)
	}

	upstreams, downstreams, err := b.buildLineage(ft)
	if err != nil {
		return fail("lineage", err)
	}

	entities, err := b.buildEntities(ft)
	if err != nil {
		return fail("entities", err)
	}

	featureTable, err := anypb.New(&v1beta2.FeatureTable{
		Namespace:  b.project,
		Entities:   entities,
		Features:   b.buildFeatures(ft),
		Attributes: &structpb.Struct{}, // ensure attributes don't get overwritten if present
		CreateTime: ft.Meta.CreatedTimestamp,
		UpdateTime: ft.Meta.LastUpdatedTimestamp,
	})
	if err != nil {
		return fail("metadata", err)
	}

	return &v1beta2.Asset{
		Urn:     plugins.CaraMLStoreURN(b.scope, b.project, ft.Spec.Name),
		Name:    ft.Spec.Name,
		Service: service,
		Type:    typ,
		Data:    featureTable,
		Lineage: &v1beta2.Lineage{
			Upstreams:   upstreams,
			Downstreams: downstreams,
		},
		Labels: ft.Spec.Labels,
	}, nil
}

func (b featureTableBuilder) buildLineage(ft *core.FeatureTable) (
	upstreams []*v1beta2.Resource, downstreams []*v1beta2.Resource, err error,
) {
	upstreams, err = b.buildUpstreams(ft)
	if err != nil {
		return nil, nil, fmt.Errorf("build lineage: %w", err)
	}

	return upstreams, nil, nil
}

func (b featureTableBuilder) buildEntities(ft *core.FeatureTable) ([]*v1beta2.FeatureTable_Entity, error) {
	entities := make([]*v1beta2.FeatureTable_Entity, 0, len(ft.Spec.Entities))
	for _, e := range ft.Spec.Entities {
		entity, ok := b.entities[e]
		if !ok {
			return nil, fmt.Errorf("entity '%s' not found in project '%s", e, b.project)
		}

		entities = append(entities, &v1beta2.FeatureTable_Entity{
			Name:        entity.Spec.Name,
			Description: entity.Spec.Description,
			Type:        entity.Spec.ValueType.String(),
			Labels:      entity.Spec.Labels,
		})
	}

	return entities, nil
}

func (b featureTableBuilder) buildFeatures(ft *core.FeatureTable) []*v1beta2.Feature {
	features := make([]*v1beta2.Feature, 0, len(ft.Spec.Features))
	for _, f := range ft.Spec.Features {
		features = append(features, &v1beta2.Feature{
			Name:     f.Name,
			DataType: f.ValueType.String(),
		})
	}

	return features
}

func (b featureTableBuilder) buildUpstreams(ft *core.FeatureTable) ([]*v1beta2.Resource, error) {
	var ups []*v1beta2.Resource
	if src := ft.Spec.BatchSource; src != nil {
		// core.DataSource_BATCH_FILE is currently unsupported for constructing lineage
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

			ups = append(ups, &v1beta2.Resource{
				Urn:     urn,
				Service: "bigquery",
				Type:    "table",
			})
		}
	}
	if src := ft.Spec.StreamSource; src != nil {
		// core.DataSource_STREAM_KINESIS is currently unsupported for constructing lineage
		switch src.Type {
		case core.DataSource_STREAM_KAFKA:
			opts := src.GetKafkaOptions()
			if opts == nil {
				return nil, errors.New("build upstream: empty kafka data source options")
			}

			ups = append(ups, &v1beta2.Resource{
				Urn:     plugins.KafkaURN(opts.BootstrapServers, opts.Topic),
				Service: "kafka",
				Type:    "topic",
			})
		}
	}

	return ups, nil
}
