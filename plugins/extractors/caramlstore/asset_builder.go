package caramlstore

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"

	"github.com/odpf/meteor/models"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/caramlstore/internal/core"
	"google.golang.org/protobuf/types/known/anypb"
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
		CreateTime: ft.Meta.CreatedTimestamp,
		UpdateTime: ft.Meta.LastUpdatedTimestamp,
	})
	if err != nil {
		return fail("metadata", err)
	}

	return &v1beta2.Asset{
		Urn:     models.NewURN(service, b.scope, typ, b.project+"-"+ft.Spec.Name),
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

		labels := map[string]string{
			"value_type":  entity.Spec.ValueType.String(),
			"description": entity.Spec.Description,
		}
		for k, v := range entity.Spec.Labels {
			labels[k] = v
		}

		entities = append(entities, &v1beta2.FeatureTable_Entity{
			Name:   entity.Spec.Name,
			Labels: labels,
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

			urn, err := mapBQTableURN(opts.TableRef)
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
				Urn:     models.NewURN("kafka", parseKafkaScope(opts.BootstrapServers), "topic", opts.Topic),
				Service: "kafka",
				Type:    "topic",
			})
		}
	}

	return ups, nil
}

func mapBQTableURN(tableRef string) (string, error) {
	projectID, datasetID, tableID, err := parseBQTableFQN(tableRef)
	if err != nil {
		return "", fmt.Errorf("map URN: %w", err)
	}

	return plugins.BigQueryURN(projectID, datasetID, tableID), nil
}

func parseBQTableFQN(fqn string) (projectID, datasetID, tableID string, err error) {
	// fqn is the ID of the table in projectID:datasetID.tableID format.
	if !strings.ContainsRune(fqn, ':') || strings.IndexRune(fqn, '.') < strings.IndexRune(fqn, ':') {
		return "", "", "", fmt.Errorf(
			"unexpected BigQuery table FQN '%s', expected in format projectID:datasetID.tableID", fqn,
		)
	}
	ss := strings.FieldsFunc(fqn, func(r rune) bool {
		return r == ':' || r == '.'
	})
	return ss[0], ss[1], ss[2], nil
}

func parseKafkaScope(servers string) string {
	if strings.IndexRune(servers, ',') > 0 {
		// there are multiple bootstrap servers, just sort and join
		ss := strings.Split(servers, ",")
		sort.Strings(ss)
		return strings.Join(ss, ",")
	}

	host, _, err := net.SplitHostPort(servers)
	if err != nil {
		return servers
	}

	return host
}
