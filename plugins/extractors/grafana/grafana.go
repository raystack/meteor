package grafana

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"
	"net/http"

	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/salt/log"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

//go:embed README.md
var summary string

// Config holds the set of configuration for the grafana extractor
type Config struct {
	BaseURL string `mapstructure:"base_url" validate:"required"`
	APIKey  string `mapstructure:"api_key" validate:"required"`
}

var sampleConfig = `
base_url: grafana_server
api_key: your_api_key`

var info = plugins.Info{
	Description:  "Dashboard list from Grafana server.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "extractor"},
}

// Extractor manages the communication with the Grafana Server
type Extractor struct {
	plugins.BaseExtractor
	client *Client
	config Config
	logger log.Logger
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
func (e *Extractor) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	// build client
	e.client = NewClient(&http.Client{}, e.config)

	return
}

// Extract checks if the extractor is configured and
// if so, then it extracts the assets from the extractor.
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	uids, err := e.client.SearchAllDashboardUIDs()
	if err != nil {
		return errors.Wrap(err, "failed to fetch dashboards")
	}
	dashboardDetails, err := e.client.GetAllDashboardDetails(uids)
	if err != nil {
		return errors.Wrap(err, "failed to fetch dashboard details")
	}

	for _, dashboardDetail := range dashboardDetails {
		dashboard, err := e.grafanaDashboardToMeteorDashboard(dashboardDetail)
		if err != nil {
			return errors.Wrap(err, "failed to build Any struct")
		}
		emit(models.NewRecord(dashboard))
	}

	return
}

// grafanaDashboardToMeteorDashboard converts a grafana dashboard to a meteor dashboard
func (e *Extractor) grafanaDashboardToMeteorDashboard(dashboard DashboardDetail) (*v1beta2.Asset, error) {
	charts := make([]*v1beta2.Chart, len(dashboard.Dashboard.Panels))
	for i, panel := range dashboard.Dashboard.Panels {
		c := e.grafanaPanelToMeteorChart(panel, dashboard.Dashboard.UID, dashboard.Meta.URL)
		charts[i] = &c
	}
	data, err := anypb.New(&v1beta2.Dashboard{
		Charts:     charts,
		Attributes: &structpb.Struct{}, // ensure attributes don't get overwritten if present
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
func (e *Extractor) grafanaPanelToMeteorChart(panel Panel, dashboardUID string, metaURL string) v1beta2.Chart {
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
