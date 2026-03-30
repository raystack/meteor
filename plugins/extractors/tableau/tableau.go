package tableau

import (
	"context"
	_ "embed"
	"net/http"

	"github.com/pkg/errors"
	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
)

//go:embed README.md
var summary string

var sampleConfig = `
host: https://server.tableau.com
version: "3.12"
username: meteor_user
password: xxxxxxxxxx
sitename: testdev550928
`

var info = plugins.Info{
	Description:  "Dashboard list from Tableau server",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "extractor"},
}

// Config that holds a set of configuration for tableau extractor
type Config struct {
	Host      string `json:"host" yaml:"host" mapstructure:"host" validate:"required"`
	Version   string `json:"version" yaml:"version" mapstructure:"version" validate:"required"` // float as string
	Username  string `json:"username" yaml:"username" mapstructure:"username"`
	Password  string `json:"password" yaml:"password" mapstructure:"password" validate:"required_with=Username"`
	AuthToken string `json:"auth_token" yaml:"auth_token" mapstructure:"auth_token" validate:"required_without=Username"`
	SiteID    string `json:"site_id" yaml:"site_id" mapstructure:"site_id" validate:"required_without=Username"`
	Sitename  string `json:"sitename" yaml:"sitename" mapstructure:"sitename"`
}

// Extractor manages the extraction of data
// from tableau server
type Extractor struct {
	plugins.BaseExtractor
	config     Config
	logger     log.Logger
	httpClient *http.Client
	client     Client
}

// Option provides extension abstraction to Extractor constructor
type Option func(*Extractor)

// WithHTTPClient assign custom http client to the Extractor constructor
func WithHTTPClient(hcl *http.Client) Option {
	return func(e *Extractor) {
		e.httpClient = hcl
	}
}

// New returns pointer to an initialized Extractor Object
func New(logger log.Logger, opts ...Option) *Extractor {
	e := &Extractor{
		logger: logger,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	for _, opt := range opts {
		opt(e)
	}

	e.client = NewClient(e.httpClient)
	return e
}

func (e *Extractor) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	err = e.client.Init(ctx, e.config)
	if err != nil {
		return
	}

	return nil
}

// Extract collects metadata from the source. The metadata is collected through the out channel
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	projects, err := e.client.GetAllProjects(ctx)
	if err != nil {
		err = errors.Wrap(err, "failed to fetch list of projects")
		return
	}

	for _, proj := range projects {
		workbooks, errC := e.client.GetDetailedWorkbooksByProjectName(ctx, proj.Name)
		if errC != nil {
			e.logger.Warn("failed to fetch list of workbook", "err", errC.Error())
			continue
		}
		for _, wb := range workbooks {
			record, errB := e.buildDashboard(wb)
			if errB != nil {
				e.logger.Error("failed to build dashboard", "data", wb, "err", errB.Error())
				continue
			}
			emit(record)
		}
	}
	return
}

func (e *Extractor) buildDashboard(wb *Workbook) (models.Record, error) {
	dashboardURN := models.NewURN("tableau", e.UrnScope, "workbook", wb.ID)

	// Build lineage edges from upstream tables
	var edges []*meteorv1beta1.Edge
	lineageURNs := e.buildLineageURNs(wb.UpstreamTables)
	for _, upstreamURN := range lineageURNs {
		edges = append(edges, models.LineageEdge(upstreamURN, dashboardURN, "tableau"))
	}

	// Build owner edge
	if wb.Owner.Email != "" {
		edges = append(edges, models.OwnerEdge(dashboardURN, "urn:user:"+wb.Owner.Email, "tableau"))
	}

	// Build charts
	charts := e.buildCharts(dashboardURN, wb)

	props := map[string]interface{}{
		"charts":       charts,
		"id":           wb.ID,
		"name":         wb.Name,
		"project_name": wb.ProjectName,
		"uri":          wb.URI,
		"owner_id":     wb.Owner.ID,
		"owner_name":   wb.Owner.Name,
		"owner_email":  wb.Owner.Email,
	}
	if !wb.CreatedAt.IsZero() {
		props["create_time"] = wb.CreatedAt.Format("2006-01-02T15:04:05Z")
	}
	if !wb.UpdatedAt.IsZero() {
		props["update_time"] = wb.UpdatedAt.Format("2006-01-02T15:04:05Z")
	}

	entity := models.NewEntity(dashboardURN, "dashboard", wb.Name, "tableau", props)
	if wb.Description != "" {
		entity.Description = wb.Description
	}

	return models.NewRecord(entity, edges...), nil
}

func (e *Extractor) buildCharts(dashboardURN string, wb *Workbook) []map[string]interface{} {
	var charts []map[string]interface{}
	for _, sh := range wb.Sheets {
		chartURN := models.NewURN("tableau", e.UrnScope, "sheet", sh.ID)
		chart := map[string]interface{}{
			"urn":           chartURN,
			"name":          sh.Name,
			"dashboard_urn": dashboardURN,
			"source":        "tableau",
			"id":            sh.ID,
		}
		if !sh.CreatedAt.IsZero() {
			chart["create_time"] = sh.CreatedAt.Format("2006-01-02T15:04:05Z")
		}
		if !sh.UpdatedAt.IsZero() {
			chart["update_time"] = sh.UpdatedAt.Format("2006-01-02T15:04:05Z")
		}
		charts = append(charts, chart)
	}
	return charts
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("tableau", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
