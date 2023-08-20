package grafana

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"
	"net/http"

	"github.com/raystack/meteor/models"
	v1beta2 "github.com/raystack/meteor/models/raystack/assets/v1beta2"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/sqlutil"
	"github.com/raystack/meteor/registry"
	"github.com/raystack/salt/log"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

//go:embed README.md
var summary string

// Config holds the set of configuration for the grafana extractor
type Config struct {
	BaseURL string  `json:"base_url" yaml:"base_url" mapstructure:"base_url" validate:"required"`
	APIKey  string  `json:"api_key" yaml:"api_key" mapstructure:"api_key" validate:"required"`
	Exclude Exclude `json:"exclude" yaml:"exclude" mapstructure:"exclude"`
}

type Exclude struct {
	Dashboards []string `json:"dashboards" yaml:"dashboards" mapstructure:"dashboards"`
	Panels     []string `json:"panels" yaml:"panels" mapstructure:"panels"`
}

var sampleConfig = `
base_url: grafana_server
api_key: your_api_key
exclude:
  dashboards: [dashboard_uid_1, dashboard_uid_2]
  panels: [dashboard_uid_3.panel_id_1]`

var info = plugins.Info{
	Description:  "Dashboard list from Grafana server.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "extractor"},
}

// Extractor manages the communication with the Grafana Server
type Extractor struct {
	plugins.BaseExtractor
	client             *Client
	config             Config
	excludedDashboards map[string]bool
	excludedPanels     map[string]bool
	logger             log.Logger
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger) *Extractor {
	e := &Extractor{
		logger: logger,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	return e
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config plugins.Config) error {
	if err := e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	// build excluded dashboards and panels map
	e.excludedDashboards = sqlutil.BuildBoolMap(e.config.Exclude.Dashboards)
	e.excludedPanels = sqlutil.BuildBoolMap(e.config.Exclude.Panels)

	// build client
	var err error
	e.client, err = NewClient(&http.Client{}, e.config)
	return err
}

// Extract checks if the extractor is configured and
// if so, then it extracts the assets from the extractor.
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	uids, err := e.client.SearchAllDashboardUIDs(ctx)
	if err != nil {
		return fmt.Errorf("fetch dashboards: %w", err)
	}

	dashboardDetails, err := e.client.GetAllDashboardDetails(ctx, uids)
	if err != nil {
		return fmt.Errorf("fetch dashboard details: %w", err)
	}

	for _, dashboardDetail := range dashboardDetails {
		// skip excluded dashboard uids
		if e.excludedDashboards[dashboardDetail.Dashboard.UID] {
			continue
		}
		dashboard, err := e.grafanaDashboardToMeteorDashboard(dashboardDetail)
		if err != nil {
			return fmt.Errorf("build Any struct: %w", err)
		}
		emit(models.NewRecord(dashboard))
	}

	return nil
}

// grafanaDashboardToMeteorDashboard converts a grafana dashboard to a meteor dashboard
func (e *Extractor) grafanaDashboardToMeteorDashboard(dashboard DashboardDetail) (*v1beta2.Asset, error) {
	charts := make([]*v1beta2.Chart, len(dashboard.Dashboard.Panels))
	for i, panel := range dashboard.Dashboard.Panels {
		// skip excluded panel ids
		panelUID := fmt.Sprintf("%s.%d", dashboard.Dashboard.UID, panel.ID)
		if e.excludedPanels[panelUID] {
			continue
		}
		c := e.grafanaPanelToMeteorChart(panel, dashboard.Dashboard.UID, dashboard.Meta.URL)
		charts[i] = &c
	}
	data, err := anypb.New(&v1beta2.Dashboard{
		Charts:     charts,
		Attributes: &structpb.Struct{},
	})
	if err != nil {
		return nil, err
	}
	return &v1beta2.Asset{
		Urn:         models.NewURN("grafana", e.UrnScope, "dashboard", dashboard.Dashboard.UID),
		Name:        dashboard.Meta.Slug,
		Type:        "dashboard",
		Service:     "grafana",
		Url:         dashboard.Meta.URL,
		Description: dashboard.Dashboard.Description,
		Data:        data,
	}, nil
}

// grafanaPanelToMeteorChart converts a grafana panel to a meteor chart
func (e *Extractor) grafanaPanelToMeteorChart(panel Panel, dashboardUID, metaURL string) v1beta2.Chart {
	var rawQuery string
	if len(panel.Targets) > 0 {
		rawQuery = panel.Targets[0].RawSQL
	}
	return v1beta2.Chart{
		Urn:             models.NewURN("grafana", e.UrnScope, "panel", fmt.Sprintf("%s.%d", dashboardUID, panel.ID)),
		Name:            panel.Title,
		Type:            panel.Type,
		Source:          "grafana",
		Description:     panel.Description,
		DataSource:      panel.DataSource,
		RawQuery:        rawQuery,
		Url:             fmt.Sprintf("%s?viewPanel=%d", metaURL, panel.ID),
		DashboardUrn:    fmt.Sprintf("grafana.%s", dashboardUID),
		DashboardSource: "grafana",
	}
}

// init registers the extractor to catalog
func init() {
	if err := registry.Extractors.Register("grafana", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
