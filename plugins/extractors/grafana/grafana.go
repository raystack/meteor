package grafana

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"
	"net/http"

	"github.com/raystack/meteor/models"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/sqlutil"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
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
		record := e.grafanaDashboardToRecord(dashboardDetail)
		emit(record)
	}

	return nil
}

// grafanaDashboardToRecord converts a grafana dashboard to a meteor Record
func (e *Extractor) grafanaDashboardToRecord(dashboard DashboardDetail) models.Record {
	var charts []map[string]any
	for _, panel := range dashboard.Dashboard.Panels {
		// skip excluded panel ids
		panelUID := fmt.Sprintf("%s.%d", dashboard.Dashboard.UID, panel.ID)
		if e.excludedPanels[panelUID] {
			continue
		}
		c := e.grafanaPanelToChart(panel, dashboard.Dashboard.UID, dashboard.Meta.URL)
		charts = append(charts, c)
	}

	urn := models.NewURN("grafana", e.UrnScope, "dashboard", dashboard.Dashboard.UID)
	props := map[string]any{
		"charts": charts,
	}
	if dashboard.Meta.URL != "" {
		props["url"] = dashboard.Meta.URL
	}
	if dashboard.Dashboard.Description != "" {
		props["description"] = dashboard.Dashboard.Description
	}

	entity := models.NewEntity(urn, "dashboard", dashboard.Meta.Slug, "grafana", props)
	return models.NewRecord(entity)
}

// grafanaPanelToChart converts a grafana panel to a chart map
func (e *Extractor) grafanaPanelToChart(panel Panel, dashboardUID, metaURL string) map[string]any {
	var rawQuery string
	if len(panel.Targets) > 0 {
		rawQuery = panel.Targets[0].RawSQL
	}
	chart := map[string]any{
		"urn":              models.NewURN("grafana", e.UrnScope, "panel", fmt.Sprintf("%s.%d", dashboardUID, panel.ID)),
		"name":             panel.Title,
		"type":             panel.Type,
		"source":           "grafana",
		"url":              fmt.Sprintf("%s?viewPanel=%d", metaURL, panel.ID),
		"dashboard_urn":    fmt.Sprintf("grafana.%s", dashboardUID),
		"dashboard_source": "grafana",
	}
	if panel.Description != "" {
		chart["description"] = panel.Description
	}
	if panel.DataSource != "" {
		chart["data_source"] = panel.DataSource
	}
	if rawQuery != "" {
		chart["raw_query"] = rawQuery
	}
	return chart
}

// init registers the extractor to catalog
func init() {
	if err := registry.Extractors.Register("grafana", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
