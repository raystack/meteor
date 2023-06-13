package optimus

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"
	"strings"

	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/extractors/optimus/client"
	"github.com/goto/meteor/registry"
	"github.com/goto/meteor/utils"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
	"github.com/goto/salt/log"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	service      = "optimus"
	sampleConfig = `host: optimus.com:80`
)

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
	Host        string `mapstructure:"host" validate:"required"`
	MaxSizeInMB int    `mapstructure:"max_size_in_mb"`
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
				data, err := e.buildJob(ctx, job, project.Name, namespace.Name)
				if err != nil {
					e.logger.Error(
						"error building job model",
						"err", err,
						"project", project.Name,
						"namespace", namespace.Name,
						"job", job.Name)
					continue
				}

				emit(models.NewRecord(data))
			}
		}
	}

	return nil
}

func (e *Extractor) buildJob(ctx context.Context, jobSpec *pb.JobSpecification, project, namespace string) (*v1beta2.Asset, error) {
	jobResp, err := e.client.GetJobTask(ctx, &pb.GetJobTaskRequest{
		ProjectName:   project,
		NamespaceName: namespace,
		JobName:       jobSpec.Name,
	})
	if err != nil {
		return nil, fmt.Errorf("fetching task: %w", err)
	}

	task := jobResp.Task
	upstreams, downstreams, err := e.buildLineage(task)
	if err != nil {
		return nil, fmt.Errorf("building lineage: %w", err)
	}

	jobID := fmt.Sprintf("%s.%s.%s", project, namespace, jobSpec.Name)
	urn := models.NewURN(service, e.UrnScope, "job", jobID)

	jobModel, err := anypb.New(&v1beta2.Job{
		Attributes: utils.TryParseMapToProto(map[string]interface{}{
			"version":          jobSpec.Version,
			"project":          project,
			"namespace":        namespace,
			"owner":            jobSpec.Owner,
			"startDate":        strOrNil(jobSpec.StartDate),
			"endDate":          strOrNil(jobSpec.EndDate),
			"interval":         jobSpec.Interval,
			"dependsOnPast":    jobSpec.DependsOnPast,
			"catchUp":          jobSpec.CatchUp,
			"taskName":         jobSpec.TaskName,
			"windowSize":       jobSpec.WindowSize,
			"windowOffset":     jobSpec.WindowOffset,
			"windowTruncateTo": jobSpec.WindowTruncateTo,
			"sql":              jobSpec.Assets["query.sql"],
			"task": map[string]interface{}{
				"name":        task.Name,
				"description": task.Description,
				"image":       task.Image,
			},
		}),
	})
	if err != nil {
		return nil, fmt.Errorf("create Any struct: %w", err)
	}

	return &v1beta2.Asset{
		Urn:         urn,
		Name:        jobSpec.Name,
		Service:     service,
		Description: jobSpec.Description,
		Type:        "job",
		Data:        jobModel,
		Owners: []*v1beta2.Owner{
			{
				Urn:   jobSpec.Owner,
				Email: jobSpec.Owner,
			},
		},
		Lineage: &v1beta2.Lineage{
			Upstreams:   upstreams,
			Downstreams: downstreams,
		},
	}, nil
}

func (e *Extractor) buildLineage(task *pb.JobTask) (upstreams, downstreams []*v1beta2.Resource, err error) {
	upstreams, err = e.buildUpstreams(task)
	if err != nil {
		return nil, nil, fmt.Errorf("build upstreams: %w", err)
	}

	downstreams, err = e.buildDownstreams(task)
	if err != nil {
		return nil, nil, fmt.Errorf("build downstreams: %w", err)
	}

	return upstreams, downstreams, nil
}

func (e *Extractor) buildUpstreams(task *pb.JobTask) ([]*v1beta2.Resource, error) {
	var upstreams []*v1beta2.Resource
	for _, dependency := range task.Dependencies {
		urn, err := plugins.BigQueryTableFQNToURN(
			strings.TrimPrefix(dependency.Dependency, "bigquery://"),
		)
		if err != nil {
			return nil, err
		}

		upstreams = append(upstreams, &v1beta2.Resource{
			Urn:     urn,
			Type:    "table",
			Service: "bigquery",
		})
	}

	return upstreams, nil
}

func (e *Extractor) buildDownstreams(task *pb.JobTask) ([]*v1beta2.Resource, error) {
	if task.Destination == nil || task.Destination.Destination == "" {
		return nil, nil
	}

	urn, err := plugins.BigQueryTableFQNToURN(
		strings.TrimPrefix(task.Destination.Destination, "bigquery://"),
	)
	if err != nil {
		return nil, err
	}

	return []*v1beta2.Resource{{
		Urn:     urn,
		Type:    "table",
		Service: "bigquery",
	}}, nil
}

func strOrNil(s string) interface{} {
	if s == "" {
		return nil
	}

	return s
}
