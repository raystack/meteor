package tableau

import (
	"context"
	_ "embed"
	"fmt"
	"net/http"

	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/meteor/utils"
	"github.com/goto/salt/log"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

//go:embed README.md
var summary string

var sampleConfig = `
host: https://server.tableau.com
version: "3.12"
identifier: my-tableau
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
	Host      string `mapstructure:"host" validate:"required"`
	Version   string `mapstructure:"version" validate:"required"` // float as string
	Username  string `mapstructure:"username"`
	Password  string `mapstructure:"password" validate:"required_with=Username"`
	AuthToken string `mapstructure:"auth_token" validate:"required_without=Username"`
	SiteID    string `mapstructure:"site_id" validate:"required_without=Username"`
	Sitename  string `mapstructure:"sitename"`
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

func (e *Extractor) Init(ctx context.Context, config plugins.Config) error {
	if err := e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	return e.client.Init(ctx, e.config)
}

// Extract collects metadata from the source. The metadata is collected through the out channel
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	projects, err := e.client.GetAllProjects(ctx)
	if err != nil {
		return fmt.Errorf("fetch list of projects: %w", err)
	}

	for _, proj := range projects {
		workbooks, errC := e.client.GetDetailedWorkbooksByProjectName(ctx, proj.Name)
		if errC != nil {
			e.logger.Warn("failed to fetch list of workbook", "err", errC.Error())
			continue
		}
		for _, wb := range workbooks {
			dashboard, errB := e.buildDashboard(wb)
			if errB != nil {
				e.logger.Error("failed to build dashboard", "data", wb, "err", errB.Error())
				continue
			}
			emit(models.NewRecord(dashboard))
		}
	}
	return nil
}

func (e *Extractor) buildDashboard(wb *Workbook) (*v1beta2.Asset, error) {
	lineages := e.buildLineage(wb.UpstreamTables)
	dashboardURN := models.NewURN("tableau", e.UrnScope, "workbook", wb.ID)
	data, err := anypb.New(&v1beta2.Dashboard{
		Charts: e.buildCharts(dashboardURN, wb),
		Attributes: utils.TryParseMapToProto(map[string]interface{}{
			"id":           wb.ID,
			"name":         wb.Name,
			"project_name": wb.ProjectName,
			"uri":          wb.URI,
			"owner_id":     wb.Owner.ID,
			"owner_name":   wb.Owner.Name,
			"owner_email":  wb.Owner.Email,
		}),
		CreateTime: timestamppb.New(wb.CreatedAt),
		UpdateTime: timestamppb.New(wb.UpdatedAt),
	})
	if err != nil {
		return nil, err
	}
	return &v1beta2.Asset{
		Urn:         dashboardURN,
		Name:        wb.Name,
		Service:     "tableau",
		Type:        "dashboard",
		Description: wb.Description,
		Data:        data,
		Owners: []*v1beta2.Owner{
			{
				Urn:   wb.Owner.Email,
				Name:  wb.Owner.Name,
				Email: wb.Owner.Email,
			},
		},
		Lineage: lineages,
	}, nil
}

func (e *Extractor) buildCharts(dashboardURN string, wb *Workbook) []*v1beta2.Chart {
	var charts []*v1beta2.Chart
	for _, sh := range wb.Sheets {
		chartURN := models.NewURN("tableau", e.UrnScope, "sheet", sh.ID)
		charts = append(charts, &v1beta2.Chart{
			Urn:          chartURN,
			Name:         sh.Name,
			DashboardUrn: dashboardURN,
			Source:       "tableau",
			Attributes: utils.TryParseMapToProto(map[string]interface{}{
				"id":   sh.ID,
				"name": sh.Name,
			}),
			CreateTime: timestamppb.New(sh.CreatedAt),
			UpdateTime: timestamppb.New(sh.UpdatedAt),
		})
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
