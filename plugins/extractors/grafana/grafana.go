package grafana

import (
	"context"
	"fmt"
	"net/http"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/assets"
	"github.com/odpf/meteor/proto/odpf/assets/common"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

var (
	configInfo = ``
	inputInfo  = `
Input:
 _________________________________________________________________________________
| Key      | Example                 | Description                   |            |
|__________|_________________________|_______________________________|____________|
|"base_url"| "http://localhost:3000" | URL of the Grafana server     | *required* |
|"api_key" | "Bearer qweruqwryqwLKJ" | API key to access Grafana API | *required* |
|__________|_________________________|_______________________________|____________|
`
	outputInfo = `
Output:
 ___________________________________________________________________________
|Field              |Sample Value                                           |
|___________________|_______________________________________________________|
|"resource.urn"     |"grafana.HzK8qNW7z"                                    |
|"resource.name"    |"new-dashboard-copy"                                   |
|"resource.service" |"grafana"                                              |
|"resource.url"     |"http://localhost:3000/d/HzK8qNW7z/new-dashboard-copy" |
|"charts"           |[]chart     											|
|___________________|_______________________________________________________|`
)

type Config struct {
	BaseURL string `mapstructure:"base_url" validate:"required"`
	APIKey  string `mapstructure:"api_key" validate:"required"`
}

type Extractor struct {
	client *Client

	// dependencies
	logger log.Logger
}

func New(logger log.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}

func (e *Extractor) GetDescription() string {
	return inputInfo + outputInfo
}

func (e *Extractor) GetSampleConfig() string {
	return configInfo
}

func (e *Extractor) Extract(ctx context.Context, configMap map[string]interface{}, out chan<- interface{}) (err error) {
	// build config
	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	// build client
	e.client = NewClient(&http.Client{}, config)

	return e.extract(out)
}

func (e *Extractor) extract(out chan<- interface{}) (err error) {
	uids, err := e.client.SearchAllDashboardUIDs()
	if err != nil {
		return
	}
	dashboardDetails, err := e.client.GetAllDashboardDetails(uids)
	if err != nil {
		return
	}

	for _, dashboardDetail := range dashboardDetails {
		out <- e.grafanaDashboardToMeteorDashboard(dashboardDetail)
	}

	return
}

func (e *Extractor) grafanaDashboardToMeteorDashboard(dashboard DashboardDetail) assets.Dashboard {
	charts := make([]*assets.Chart, len(dashboard.Dashboard.Panels))
	for i, panel := range dashboard.Dashboard.Panels {
		c := e.grafanaPanelToMeteorChart(panel, dashboard.Dashboard.UID, dashboard.Meta.URL)
		charts[i] = &c
	}
	return assets.Dashboard{
		Resource: &common.Resource{
			Urn:     fmt.Sprintf("grafana.%s", dashboard.Dashboard.UID),
			Name:    dashboard.Meta.Slug,
			Service: "grafana",
			Url:     dashboard.Meta.URL,
		},
		Description: dashboard.Dashboard.Description,
		Charts:      charts,
	}
}

func (e *Extractor) grafanaPanelToMeteorChart(panel Panel, dashboardUID string, metaURL string) assets.Chart {
	var rawQuery string
	if len(panel.Targets) > 0 {
		rawQuery = panel.Targets[0].RawSQL
	}
	return assets.Chart{
		Urn:             fmt.Sprintf("%s.%d", dashboardUID, panel.ID),
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

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("grafana", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
