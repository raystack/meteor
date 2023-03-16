package optimus

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"
	"strings"

	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/meteor/utils"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
	"github.com/goto/salt/log"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/anypb"
)

//go:embed README.md
var summary string

// Config holds the set of configuration for the bigquery extractor
type Config struct {
	Host        string `mapstructure:"host" validate:"required"`
	MaxSizeInMB int    `mapstructure:"max_size_in_mb"`
}

var sampleConfig = `host: optimus.com:80`

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
	client Client
}

func New(logger log.Logger, client Client) *Extractor {
	e := &Extractor{
		logger: logger,
		client: client,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	return e
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	if err := e.client.Connect(ctx, e.config.Host, e.config.MaxSizeInMB); err != nil {
		return errors.Wrap(err, "error connecting to host")
	}

	return
}

// Extract checks if the table is valid and extracts the table schema
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	defer e.client.Close()

	projResp, err := e.client.ListProjects(ctx, &pb.ListProjectsRequest{})
	if err != nil {
		return errors.Wrap(err, "error fetching projects")
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

func (e *Extractor) buildJob(ctx context.Context, jobSpec *pb.JobSpecification, project, namespace string) (asset *v1beta2.Asset, err error) {
	jobResp, err := e.client.GetJobTask(ctx, &pb.GetJobTaskRequest{
		ProjectName:   project,
		NamespaceName: namespace,
		JobName:       jobSpec.Name,
	})
	if err != nil {
		err = errors.Wrap(err, "error fetching task")
		return
	}

	task := jobResp.Task
	upstreams, downstreams, err := e.buildLineage(task)
	if err != nil {
		err = errors.Wrap(err, "error building lineage")
		return
	}

	jobID := fmt.Sprintf("%s.%s.%s", project, namespace, jobSpec.Name)
	urn := models.NewURN(service, e.UrnScope, "job", jobID)

	jobModel, err := anypb.New(&v1beta2.Job{
		Attributes: utils.TryParseMapToProto(map[string]interface{}{
			"version":          jobSpec.Version,
			"project":          project,
			"namespace":        namespace,
			"owner":            jobSpec.Owner,
			"startDate":        jobSpec.StartDate,
			"endDate":          jobSpec.EndDate,
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
		err = fmt.Errorf("error creating Any struct: %w", err)
	}

	asset = &v1beta2.Asset{
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
	}

	return
}

func (e *Extractor) buildLineage(task *pb.JobTask) (upstreams, downstreams []*v1beta2.Resource, err error) {
	upstreams, err = e.buildUpstreams(task)
	if err != nil {
		err = errors.Wrap(err, "error building upstreams")
		return
	}
	downstreams, err = e.buildDownstreams(task)
	if err != nil {
		err = errors.Wrap(err, "error building downstreams")
		return
	}

	return
}

func (e *Extractor) buildUpstreams(task *pb.JobTask) (upstreams []*v1beta2.Resource, err error) {
	for _, dependency := range task.Dependencies {
		var urn string
		urn, err = e.mapURN(dependency.Dependency)
		if err != nil {
			return
		}

		upstreams = append(upstreams, &v1beta2.Resource{
			Urn:     urn,
			Type:    "table",
			Service: "bigquery",
		})
	}

	return
}

func (e *Extractor) buildDownstreams(task *pb.JobTask) (downstreams []*v1beta2.Resource, err error) {
	if task.Destination == nil {
		return
	}

	var urn string
	urn, err = e.mapURN(task.Destination.Destination)
	if err != nil {
		return
	}

	downstreams = append(downstreams, &v1beta2.Resource{
		Urn:     urn,
		Type:    "table",
		Service: "bigquery",
	})

	return
}

func (e *Extractor) mapURN(optimusURN string) (tableURN string, err error) {
	err = fmt.Errorf("could not map urn \"%s\"", optimusURN)

	// sample optimusURN = "bigquery://projectA:datasetB.tableC"
	bigqueryID := strings.TrimPrefix(optimusURN, "bigquery://") // "projectA:datasetB.tableC"

	comps := strings.Split(bigqueryID, ":") // ["projectA", "datasetB.tableC"]
	if len(comps) != 2 {
		return
	}
	projectID := comps[0]                          // "projectA"
	datasetTableID := strings.Split(comps[1], ".") // ["datasetB", "tableC"]
	if len(comps) != 2 {
		return
	}
	datasetID := datasetTableID[0] // "datasetB"
	tableID := datasetTableID[1]   // "tableC"

	return plugins.BigQueryURN(projectID, datasetID, tableID), nil
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("optimus", func() plugins.Extractor {
		return New(plugins.GetLog(), newClient())
	}); err != nil {
		panic(err)
	}
}
