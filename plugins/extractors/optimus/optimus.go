package optimus

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"
	"strings"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/models/odpf/assets/facets"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
)

//go:embed README.md
var summary string

// Config hold the set of configuration for the bigquery extractor
type Config struct {
	Host string `mapstructure:"host" validate:"required"`
}

var sampleConfig = `
host: optimus.com:80`

// Extractor manages the communication with the bigquery service
type Extractor struct {
	logger log.Logger
	config Config
	client Client
}

func New(logger log.Logger, client Client) *Extractor {
	return &Extractor{
		logger: logger,
		client: client,
	}
}

// Info returns the detailed information about the extractor
func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description:  "Optimus' jobs metadata",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"optimus", "bigquery", "job", "extractor"},
	}
}

// Validate validates the configuration of the extractor
func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, configMap map[string]interface{}) (err error) {
	if err := utils.BuildConfig(configMap, &e.config); err != nil {
		return plugins.InvalidConfigError{}
	}

	if err := e.client.Connect(ctx, e.config.Host); err != nil {
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
				ProjectName: project.Name,
				Namespace:   namespace.Name,
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

func (e *Extractor) buildJob(ctx context.Context, jobSpec *pb.JobSpecification, project, namespace string) (job *assets.Job, err error) {
	jobResp, err := e.client.GetJobTask(ctx, &pb.GetJobTaskRequest{
		ProjectName: project,
		Namespace:   namespace,
		JobName:     jobSpec.Name,
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
	urn := models.JobURN(service, e.config.Host, jobID)
	job = &assets.Job{
		Resource: &common.Resource{
			Urn:         urn,
			Name:        jobSpec.Name,
			Service:     service,
			Description: jobSpec.Description,
		},
		Ownership: &facets.Ownership{
			Owners: []*facets.Owner{
				{
					Urn:  jobSpec.Owner,
					Name: jobSpec.Owner,
				},
			},
		},
		Lineage: &facets.Lineage{
			Upstreams:   upstreams,
			Downstreams: downstreams,
		},
		Properties: &facets.Properties{
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
		},
	}

	return
}

func (e *Extractor) buildLineage(task *pb.JobTask) (upstreams, downstreams []*common.Resource, err error) {
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

func (e *Extractor) buildUpstreams(task *pb.JobTask) (upstreams []*common.Resource, err error) {
	for _, dependency := range task.Dependencies {
		var urn string
		urn, err = e.mapURN(dependency.Dependency)
		if err != nil {
			return
		}

		upstreams = append(upstreams, &common.Resource{
			Urn:     urn,
			Type:    "table",
			Service: "bigquery",
		})
	}

	return
}

func (e *Extractor) buildDownstreams(task *pb.JobTask) (downstreams []*common.Resource, err error) {
	if task.Destination == nil {
		return
	}

	var urn string
	urn, err = e.mapURN(task.Destination.Destination)
	if err != nil {
		return
	}

	downstreams = append(downstreams, &common.Resource{
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

	return models.TableURN("bigquery", projectID, datasetID, tableID), nil
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("optimus", func() plugins.Extractor {
		return New(plugins.GetLog(), newClient())
	}); err != nil {
		panic(err)
	}
}
