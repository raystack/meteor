package optimus

import (
	"context"
	_ "embed" // used to print the embedded assets
	"errors"
	"fmt"
	"strings"

	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/extractors/optimus/client"
	"github.com/raystack/meteor/registry"
	pb "github.com/raystack/optimus/protos/raystack/optimus/core/v1beta1"
	log "github.com/raystack/salt/observability/logger"
)

const (
	service          = "optimus"
	sampleConfig     = `host: optimus.com:80`
	prefixBigQuery   = "bigquery://"
	prefixMaxcompute = "maxcompute://"
)

var errInvalidDependency = errors.New("invalid dependency format")

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("optimus", func() plugins.Extractor {
		return New(plugins.GetLog(), client.New())
	}); err != nil {
		panic(err)
	}
}

//go:embed README.md
var summary string

// Config holds the set of configuration for the bigquery extractor
type Config struct {
	Host        string `json:"host" yaml:"host" mapstructure:"host" validate:"required"`
	MaxSizeInMB int    `json:"max_size_in_mb" yaml:"max_size_in_mb" mapstructure:"max_size_in_mb"`
}

var info = plugins.Info{
	Description:  "Optimus' jobs metadata",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"optimus", "bigquery"},
}

// Extractor manages the communication with the bigquery service
type Extractor struct {
	plugins.BaseExtractor
	logger log.Logger
	config Config
	client client.Client
}

func New(l log.Logger, c client.Client) *Extractor {
	e := &Extractor{
		logger: l,
		client: c,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	return e
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config plugins.Config) error {
	if err := e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	if err := e.client.Connect(ctx, e.config.Host, e.config.MaxSizeInMB); err != nil {
		return fmt.Errorf("connect to host: %w", err)
	}

	return nil
}

// Extract checks if the table is valid and extracts the table schema
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	defer e.client.Close()

	projResp, err := e.client.ListProjects(ctx, &pb.ListProjectsRequest{})
	if err != nil {
		return fmt.Errorf("fetch projects: %w", err)
	}

	for _, project := range projResp.Projects {
		nspaceResp, err := e.client.ListProjectNamespaces(ctx, &pb.ListProjectNamespacesRequest{
			ProjectName: project.Name,
		})
		if err != nil {
			e.logger.Error("error fetching namespace list", "project", project.Name, "err", err)
			continue
		}

		for _, namespace := range nspaceResp.Namespaces {
			jobResp, err := e.client.ListJobSpecification(ctx, &pb.ListJobSpecificationRequest{
				ProjectName:   project.Name,
				NamespaceName: namespace.Name,
			})
			if err != nil {
				e.logger.Error("error fetching job list", "err", err, "project", project.Name, "namespace", namespace.Name)
				continue
			}

			for _, job := range jobResp.Jobs {
				record, err := e.buildJob(ctx, job, project.Name, namespace.Name)
				if err != nil {
					e.logger.Error(
						"error building job model",
						"err", err,
						"project", project.Name,
						"namespace", namespace.Name,
						"job", job.Name)
					continue
				}

				emit(record)
			}
		}
	}

	return nil
}

func (e *Extractor) buildJob(ctx context.Context, jobSpec *pb.JobSpecification, project, namespace string) (models.Record, error) {
	jobResp, err := e.client.GetJobTask(ctx, &pb.GetJobTaskRequest{
		ProjectName:   project,
		NamespaceName: namespace,
		JobName:       jobSpec.Name,
	})
	if err != nil {
		return models.Record{}, fmt.Errorf("fetching task: %w", err)
	}

	task := jobResp.Task
	upstreamURNs, downstreamURNs, err := e.buildLineageURNs(task)
	if err != nil {
		return models.Record{}, fmt.Errorf("building lineage: %w", err)
	}

	jobID := fmt.Sprintf("%s.%s.%s", project, namespace, jobSpec.Name)
	urn := models.NewURN(service, e.UrnScope, "job", jobID)

	// Build edges for lineage
	var edges []*meteorv1beta1.Edge
	for _, upstreamURN := range upstreamURNs {
		edges = append(edges, models.LineageEdge(upstreamURN, urn, service))
	}
	for _, downstreamURN := range downstreamURNs {
		edges = append(edges, models.LineageEdge(urn, downstreamURN, service))
	}

	// Build owner edge
	if jobSpec.Owner != "" {
		edges = append(edges, models.OwnerEdge(urn, "urn:user:"+jobSpec.Owner, service))
	}

	props := map[string]interface{}{
		"version":          jobSpec.Version,
		"project":          project,
		"project_id":       project,
		"namespace":        namespace,
		"owner":            jobSpec.Owner,
		"interval":         jobSpec.Interval,
		"dependsOnPast":    jobSpec.DependsOnPast,
		"taskName":         jobSpec.TaskName,
		"windowSize":       jobSpec.WindowSize,
		"windowOffset":     jobSpec.WindowOffset,
		"windowTruncateTo": jobSpec.WindowTruncateTo,
		"task": map[string]interface{}{
			"name":        task.Name,
			"description": task.Description,
			"image":       task.Image,
		},
	}
	if startDate := strOrNil(jobSpec.StartDate); startDate != nil {
		props["startDate"] = startDate
	}
	if endDate := strOrNil(jobSpec.EndDate); endDate != nil {
		props["endDate"] = endDate
	}
	if sql, ok := jobSpec.Assets["query.sql"]; ok && sql != "" {
		props["sql"] = sql
	}

	entity := models.NewEntity(urn, "job", jobSpec.Name, service, props)
	if jobSpec.Description != "" {
		entity.Description = jobSpec.Description
	}

	return models.NewRecord(entity, edges...), nil
}

func (e *Extractor) buildLineageURNs(task *pb.JobTask) (upstreamURNs, downstreamURNs []string, err error) {
	upstreamURNs, err = e.buildUpstreamURNs(task)
	if err != nil {
		return nil, nil, fmt.Errorf("build upstreams: %w", err)
	}

	downstreamURNs, err = e.buildDownstreamURNs(task)
	if err != nil {
		return nil, nil, fmt.Errorf("build downstreams: %w", err)
	}

	return upstreamURNs, downstreamURNs, nil
}

func (e *Extractor) buildUpstreamURNs(task *pb.JobTask) ([]string, error) {
	var urns []string
	for _, dependency := range task.Dependencies {
		urn, err := createResourceURN(dependency.Dependency)
		if err != nil {
			e.logger.Warn("skipping upstream dependency", "dependency", dependency.Dependency, "err", err)
			continue
		}

		urns = append(urns, urn)
	}

	return urns, nil
}

func (e *Extractor) buildDownstreamURNs(task *pb.JobTask) ([]string, error) {
	if task.Destination == nil || task.Destination.Destination == "" {
		return nil, nil
	}

	urn, err := createResourceURN(task.Destination.Destination)
	if err != nil {
		return nil, err
	}

	return []string{urn}, nil
}

func createResourceURN(dependency string) (string, error) {
	switch {
	case strings.HasPrefix(dependency, prefixBigQuery):
		return createBigQueryResourceURN(strings.TrimPrefix(dependency, prefixBigQuery))
	case strings.HasPrefix(dependency, prefixMaxcompute):
		return createMaxComputeResourceURN(strings.TrimPrefix(dependency, prefixMaxcompute))
	default:
		return "", fmt.Errorf("%w: %s", errInvalidDependency, dependency)
	}
}

func createBigQueryResourceURN(fqn string) (string, error) {
	urn, err := plugins.BigQueryTableFQNToURN(fqn)
	if err != nil {
		return "", err
	}
	return urn, nil
}

func createMaxComputeResourceURN(fqn string) (string, error) {
	urn, err := plugins.MaxComputeTableFQNToURN(fqn)
	if err != nil {
		return "", err
	}
	return urn, nil
}

func strOrNil(s string) interface{} {
	if s == "" {
		return nil
	}

	return s
}
