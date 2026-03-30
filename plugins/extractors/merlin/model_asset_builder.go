package merlin

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/extractors/merlin/internal/merlin"
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

func (b modelBuilder) buildRecord() (models.Record, error) {
	fail := func(step string, err error) (models.Record, error) {
		return models.Record{}, fmt.Errorf(
			"build %s for model '%d' in project '%d': %w",
			step, b.model.ID, b.project.ID, err,
		)
	}

	versions, err := b.buildVersions()
	if err != nil {
		return fail("versions", err)
	}

	urls := b.buildEndpointURLs()

	urn := models.NewURN(service, b.scope, typ, fmt.Sprintf("%s.%s", b.project.Name, b.model.Name))

	// Build edges
	var edges []*meteorv1beta1.Edge

	// Lineage edges (upstreams)
	upstreamURNs, err := b.buildUpstreamURNs()
	if err != nil {
		return fail("lineage", err)
	}
	for _, upstreamURN := range upstreamURNs {
		edges = append(edges, models.LineageEdge(upstreamURN, urn, service))
	}

	// Owner edges
	for _, ownerURN := range b.buildOwnerURNs() {
		edges = append(edges, models.OwnerEdge(urn, ownerURN, service))
	}

	props := map[string]interface{}{
		"namespace":            b.project.Name,
		"flavor":               b.model.Type,
		"merlin_project_id":    b.project.ID,
		"mlflow_experiment_id": b.model.MlflowExperimentID,
		"mlflow_experiment_url": b.model.MlflowURL,
	}
	if len(urls) > 0 {
		props["endpoint_urls"] = urls
	}
	if len(versions) > 0 {
		props["versions"] = versions
	}
	if !b.model.CreatedAt.IsZero() {
		props["create_time"] = b.model.CreatedAt.Format("2006-01-02T15:04:05Z")
	}
	if !b.model.UpdatedAt.IsZero() {
		props["update_time"] = b.model.UpdatedAt.Format("2006-01-02T15:04:05Z")
	}

	// Labels
	labels := b.buildLabels()
	if len(labels) > 0 {
		props["labels"] = labels
	}

	if len(urls) > 0 {
		props["url"] = urls[0]
	}
	entity := models.NewEntity(urn, typ, b.model.Name, service, props)

	return models.NewRecord(entity, edges...), nil
}

// Based on https://github.com/gojek/merlin/blob/v0.24.0/api/pkg/transformer/spec/feast.pb.go#L350
type featureTable struct {
	Name    string `json:"name"`
	Project string `json:"project"`
}

func (b modelBuilder) buildUpstreamURNs() ([]string, error) {
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

	var urns []string
	for ft := range fts {
		urns = append(urns, plugins.CaraMLStoreURN(b.scope, ft.Project, ft.Name))
	}
	// For testability, we need a deterministic output. So sort the urns
	sort.Strings(urns)
	return urns, nil
}

func (b modelBuilder) buildVersions() ([]map[string]interface{}, error) {
	var versions []map[string]interface{}
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

			version := map[string]interface{}{
				"status":  vEp.Status,
				"version": strconv.FormatInt(mdlv.ID, 10),
				"endpoint_id":          endpoint.ID,
				"mlflow_run_id":        mdlv.MlflowRunID,
				"mlflow_run_url":       mdlv.MlflowURL,
				"endpoint_url":         endpoint.URL,
				"version_endpoint_url": vEp.URL,
				"monitoring_url":       vEp.MonitoringURL,
				"message":              vEp.Message,
				"environment_name":     endpoint.EnvironmentName,
				"deployment_mode":      vEp.DeploymentMode,
				"service_name":         vEp.ServiceName,
				"weight":               dest.Weight,
			}
			if len(vEp.EnvVars) > 0 {
				envVars := make(map[string]string, len(vEp.EnvVars))
				for _, ev := range vEp.EnvVars {
					envVars[ev.Name] = ev.Value
				}
				version["env_vars"] = envVars
			}
			version["transformer"] = buildTransformerMap(vEp.Transformer)
			if len(mdlv.Labels) > 0 {
				version["labels"] = mdlv.Labels
			}
			if !mdlv.CreatedAt.IsZero() {
				version["create_time"] = mdlv.CreatedAt.Format("2006-01-02T15:04:05Z")
			}
			if !mdlv.UpdatedAt.IsZero() {
				version["update_time"] = mdlv.UpdatedAt.Format("2006-01-02T15:04:05Z")
			}

			versions = append(versions, version)
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

func (b modelBuilder) buildOwnerURNs() []string {
	var urns []string
	emails := make(map[string]struct{}, len(b.project.Administrators))
	for _, admin := range b.project.Administrators {
		if _, ok := emails[admin]; ok {
			continue
		}

		urns = append(urns, "urn:user:"+admin)
		emails[admin] = struct{}{}
	}
	return urns
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

func buildTransformerMap(tr merlin.Transformer) map[string]interface{} {
	attrs := map[string]interface{}{
		"enabled": tr.Enabled,
	}
	if !tr.Enabled {
		return attrs
	}

	attrs["type"] = tr.TransformerType
	attrs["image"] = tr.Image
	if tr.Command != "" {
		attrs["command"] = tr.Command
		attrs["args"] = tr.Args
	}

	if len(tr.EnvVars) > 0 {
		envVars := make(map[string]string, len(tr.EnvVars))
		for _, ev := range tr.EnvVars {
			envVars[ev.Name] = ev.Value
		}
		attrs["env_vars"] = envVars
	}

	return attrs
}
