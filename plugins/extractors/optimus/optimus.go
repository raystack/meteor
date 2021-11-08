package optimus

import (
	"context"
	_ "embed" // used to print the embedded assets
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/models/odpf/assets/facets"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
)

//go:embed README.md
var summary string

const (
	service = "optimus"
)

// Config hold the set of configuration for the bigquery extractor
type Config struct {
	Host string `mapstructure:"host" validate:"required"`
}

var sampleConfig = `
host: https://optimus.com`

// Extractor manages the communication with the bigquery service
type Extractor struct {
	logger  log.Logger
	config  Config
	baseURL *url.URL
	client  *http.Client
}

func New(logger log.Logger, httpClient *http.Client) *Extractor {
	return &Extractor{
		logger: logger,
		client: httpClient,
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

	e.baseURL, err = url.Parse(e.config.Host)
	if err != nil {
		return errors.Wrap(err, "error parsing host")
	}

	if _, err := e.doRequest("/ping"); err != nil {
		return errors.Wrap(err, "error checking connection")
	}

	return
}

// Extract checks if the table is valid and extracts the table schema
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	var projResp GetProjectsResponse
	if err := e.fetch("/api/v1/project", &projResp); err != nil {
		return errors.Wrap(err, "error fetching projects")
	}

	for _, project := range projResp.Projects {
		var nspaceResp GetNamespacesResponse
		uri := fmt.Sprintf("/api/v1/project/%s/namespace", project.Name)
		if err := e.fetch(uri, &nspaceResp); err != nil {
			e.logger.Error("error fetching namespace list", "project", project.Name, "err", err)
			continue
		}

		for _, namespace := range nspaceResp.Namespaces {
			var jobResp GetJobsResponse
			uri := fmt.Sprintf("/api/v1/project/%s/job?namespace=%s", project.Name, namespace.Name)
			if err := e.fetch(uri, &jobResp); err != nil {
				e.logger.Error("error fetching job list", "err", err, "project", project.Name, "namespace", namespace.Name)
				continue
			}

			for _, job := range jobResp.Jobs {
				data, err := e.buildJob(job, project.Name, namespace.Name)
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

func (e *Extractor) buildJob(optJob Job, project, namespace string) (job *assets.Job, err error) {
	jobID := fmt.Sprintf("%s.%s.%s", project, namespace, optJob.Name)
	urn := models.JobURN(service, e.baseURL.String(), jobID)

	var resp GetJobTaskResponse
	url := fmt.Sprintf("/api/v1/project/%s/namespace/%s/job/%s/task", project, namespace, optJob.Name)
	if err = e.fetch(url, &resp); err != nil {
		err = errors.Wrap(err, "error fetching task")
		return
	}
	task := resp.Task
	upstreams, downstreams, err := e.buildLineage(task)
	if err != nil {
		err = errors.Wrap(err, "error building lineage")
		return
	}

	job = &assets.Job{
		Resource: &common.Resource{
			Urn:         urn,
			Name:        optJob.Name,
			Service:     service,
			Description: optJob.Description,
		},
		Ownership: &facets.Ownership{
			Owners: []*facets.Owner{
				{
					Urn:  optJob.Owner,
					Name: optJob.Owner,
				},
			},
		},
		Lineage: &facets.Lineage{
			Upstreams:   upstreams,
			Downstreams: downstreams,
		},
		Properties: &facets.Properties{
			Attributes: utils.TryParseMapToProto(map[string]interface{}{
				"version":          optJob.Version,
				"project":          project,
				"namespace":        namespace,
				"owner":            optJob.Owner,
				"startDate":        optJob.StartDate,
				"endDate":          optJob.EndDate,
				"interval":         optJob.Interval,
				"dependsOnPast":    optJob.DependsOnPast,
				"catchUp":          optJob.CatchUp,
				"taskName":         optJob.TaskName,
				"windowSize":       optJob.WindowSize,
				"windowOffset":     optJob.WindowOffset,
				"windowTruncateTo": optJob.WindowTruncateTo,
				"sql":              optJob.Assets.QuerySQL,
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

func (e *Extractor) buildLineage(task JobTask) (upstreams, downstreams []*common.Resource, err error) {
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

func (e *Extractor) buildUpstreams(task JobTask) (upstreams []*common.Resource, err error) {
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

func (e *Extractor) buildDownstreams(task JobTask) (downstreams []*common.Resource, err error) {
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

	comps := strings.Split(optimusURN, "://")
	if len(comps) < 2 {
		return
	}

	bqURNComps := strings.Split(comps[1], ".")
	if len(bqURNComps) < 3 {
		return
	}

	return models.TableURN("bigquery", bqURNComps[0], bqURNComps[1], bqURNComps[2]), nil
}

func (e *Extractor) fetch(uri string, data interface{}) error {
	body, err := e.doRequest(uri)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(body, &data); err != nil {
		return errors.Wrap(err, "error unmarshalling body")
	}

	return nil
}

func (e *Extractor) doRequest(uri string) (body []byte, err error) {
	u, err := e.baseURL.Parse(uri)
	if err != nil {
		err = errors.Wrapf(err, "error parsing uri: %s", uri)
		return
	}

	resp, err := e.client.Get(u.String())
	if err != nil {
		err = errors.Wrap(err, "error building request")
		return
	}
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		err = errors.Wrap(err, "error reading response body")
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("got %d status code from %s: %s", resp.StatusCode, u.String(), string(body))
		return
	}

	return
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("optimus", func() plugins.Extractor {
		return New(plugins.GetLog(), &http.Client{})
	}); err != nil {
		panic(err)
	}
}
