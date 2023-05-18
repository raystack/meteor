package merlin

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/extractors/merlin/internal/merlin"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	service = "merlin"
	typ     = "model"
)

type modelBuilder struct {
	scope    string
	project  merlin.Project
	model    merlin.Model
	versions map[int64]merlin.ModelVersion
}

func (b modelBuilder) buildAsset() (*v1beta2.Asset, error) {
	fail := func(step string, err error) (*v1beta2.Asset, error) {
		return nil, fmt.Errorf(
			"build %s for model '%d' in project '%d': %w",
			step, b.model.ID, b.project.ID, err,
		)
	}

	versions, err := b.buildVersions()
	if err != nil {
		return fail("versions", err)
	}

	urls := b.buildEndpointURLs()

	model, err := anypb.New(&v1beta2.Model{
		Namespace: b.project.Name,
		Flavor:    b.model.Type,
		Versions:  versions,
		Attributes: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"merlin_project_id":     intToValue(b.project.ID),
				"mlflow_experiment_id":  intToValue(b.model.MlflowExperimentID),
				"mlflow_experiment_url": structpb.NewStringValue(b.model.MlflowURL),
				"endpoint_urls":         stringSliceToValue(urls),
			},
		},
		CreateTime: timestamppb.New(b.model.CreatedAt),
		UpdateTime: timestamppb.New(b.model.UpdatedAt),
	})
	if err != nil {
		return fail("encode model metadata", err)
	}

	lineage, err := b.buildLineage()
	if err != nil {
		return fail("lineage", err)
	}

	return &v1beta2.Asset{
		Urn:     models.NewURN(service, b.scope, typ, fmt.Sprintf("%s.%s", b.project.Name, b.model.Name)),
		Name:    b.model.Name,
		Service: service,
		Type:    typ,
		Url:     urls[0],
		Data:    model,
		Owners:  b.buildOwners(),
		Lineage: lineage,
		Labels:  b.buildLabels(),
	}, nil
}

func (b modelBuilder) buildLineage() (*v1beta2.Lineage, error) {
	upstreams, err := b.buildUpstreams()
	if err != nil {
		return nil, fmt.Errorf("build upstreams: %w", err)
	}

	return &v1beta2.Lineage{Upstreams: upstreams}, nil
}

// Based on https://github.com/gojek/merlin/blob/v0.24.0/api/pkg/transformer/spec/feast.pb.go#L350
type featureTable struct {
	Name    string `json:"name"`
	Project string `json:"project"`
}

func (b modelBuilder) buildUpstreams() ([]*v1beta2.Resource, error) {
	fts := make(map[featureTable]struct{})
	for _, endpoint := range b.model.Endpoints {
		for _, dest := range endpoint.Rule.Destinations {
			if dest.VersionEndpoint == nil {
				continue
			}

			specs, err := decodeFeatureTableSpecs(dest.VersionEndpoint.Transformer)
			if err != nil {
				return nil, err
			}

			for _, f := range specs {
				fts[f] = struct{}{}
			}
		}
	}

	var upstreams []*v1beta2.Resource
	for ft := range fts {
		upstreams = append(upstreams, &v1beta2.Resource{
			Urn:     plugins.CaraMLStoreURN(b.scope, ft.Project, ft.Name),
			Service: "caramlstore",
			Type:    "feature_table",
		})
	}
	// For testability, we need a deterministic output. So sort the upstreams
	sort.Slice(upstreams, func(i, j int) bool {
		return upstreams[i].Urn < upstreams[j].Urn
	})
	return upstreams, nil
}

func (b modelBuilder) buildVersions() ([]*v1beta2.ModelVersion, error) {
	var versions []*v1beta2.ModelVersion
	for _, endpoint := range b.model.Endpoints {
		for _, dest := range endpoint.Rule.Destinations {
			vEp := dest.VersionEndpoint
			if vEp == nil {
				continue
			}

			mdlv, ok := b.versions[vEp.VersionID]
			if !ok {
				return nil, fmt.Errorf("model version not found: %d", vEp.VersionID)
			}

			versions = append(versions, &v1beta2.ModelVersion{
				Status:  vEp.Status,
				Version: strconv.FormatInt(mdlv.ID, 10),
				Attributes: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"endpoint_id":          intToValue(endpoint.ID),
						"mlflow_run_id":        structpb.NewStringValue(mdlv.MlflowRunID),
						"mlflow_run_url":       structpb.NewStringValue(mdlv.MlflowURL),
						"endpoint_url":         structpb.NewStringValue(endpoint.URL),
						"version_endpoint_url": structpb.NewStringValue(vEp.URL),
						"monitoring_url":       structpb.NewStringValue(vEp.MonitoringURL),
						"message":              structpb.NewStringValue(vEp.Message),
						"environment_name":     structpb.NewStringValue(endpoint.EnvironmentName),
						"deployment_mode":      structpb.NewStringValue(vEp.DeploymentMode),
						"service_name":         structpb.NewStringValue(vEp.ServiceName),
						"env_vars":             envVarsToValue(vEp.EnvVars),
						"transformer":          transformerAttrs(vEp.Transformer),
						"weight":               intToValue(dest.Weight),
					},
				},
				Labels:     mdlv.Labels,
				CreateTime: timestamppb.New(mdlv.CreatedAt),
				UpdateTime: timestamppb.New(mdlv.UpdatedAt),
			})
		}
	}

	return versions, nil
}

func (b modelBuilder) buildEndpointURLs() []string {
	var urls []string
	for _, endpoint := range b.model.Endpoints {
		urls = append(urls, endpoint.URL)
	}
	return urls
}

func (b modelBuilder) buildOwners() []*v1beta2.Owner {
	var owners []*v1beta2.Owner
	emails := make(map[string]struct{}, len(b.project.Administrators))
	for _, admin := range b.project.Administrators {
		if _, ok := emails[admin]; ok {
			continue
		}

		owners = append(owners, &v1beta2.Owner{
			Urn:   admin,
			Email: admin,
		})
		emails[admin] = struct{}{}
	}
	return owners
}

func (b modelBuilder) buildLabels() map[string]string {
	labels := map[string]string{
		"team":   b.project.Team,
		"stream": b.project.Stream,
	}
	for _, l := range b.project.Labels {
		labels[l.Key] = l.Value
	}

	return labels
}

func decodeFeatureTableSpecs(tr merlin.Transformer) ([]featureTable, error) {
	if !tr.Enabled || tr.TransformerType != "standard" {
		return nil, nil
	}

	for _, envvar := range tr.EnvVars {
		if envvar.Name != "FEAST_FEATURE_TABLE_SPECS_JSONS" {
			continue
		}

		var specs []featureTable
		if err := json.Unmarshal(([]byte)(envvar.Value), &specs); err != nil {
			return nil, fmt.Errorf("decode FEAST_FEATURE_TABLE_SPECS_JSONS %w", err)
		}

		return specs, nil
	}

	return nil, nil
}

func transformerAttrs(tr merlin.Transformer) *structpb.Value {
	attrs := map[string]*structpb.Value{
		"enabled": structpb.NewBoolValue(tr.Enabled),
	}
	if !tr.Enabled {
		return structpb.NewStructValue(&structpb.Struct{Fields: attrs})
	}

	attrs["type"] = structpb.NewStringValue(tr.TransformerType)
	attrs["image"] = structpb.NewStringValue(tr.Image)
	if tr.Command != "" {
		attrs["command"] = structpb.NewStringValue(tr.Command)
		attrs["args"] = structpb.NewStringValue(tr.Args)
	}

	attrs["env_vars"] = envVarsToValue(tr.EnvVars)

	return structpb.NewStructValue(&structpb.Struct{Fields: attrs})
}

func stringSliceToValue(urls []string) *structpb.Value {
	var l structpb.ListValue
	for _, u := range urls {
		l.Values = append(l.Values, structpb.NewStringValue(u))
	}

	return structpb.NewListValue(&l)
}

func intToValue(n int64) *structpb.Value { return structpb.NewNumberValue((float64)(n)) }

func envVarsToValue(vars []merlin.EnvVar) *structpb.Value {
	if len(vars) == 0 {
		return structpb.NewNullValue()
	}

	attrs := make(map[string]*structpb.Value, len(vars))
	for _, envvar := range vars {
		attrs[envvar.Name] = structpb.NewStringValue(envvar.Value)
	}

	return structpb.NewStructValue(&structpb.Struct{Fields: attrs})
}
