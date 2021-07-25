package grafana

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins"
	meteorMeta "github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/utils"
)

type Config struct {
	BaseURL string `mapstructure:"base_url" validate:"required"`
	APIKey  string `mapstructure:"api_key" validate:"required"`
}

type Extractor struct {
	httpClient *http.Client
	logger     plugins.Logger
}

func (e *Extractor) Extract(ctx context.Context, configMap map[string]interface{}, out chan<- interface{}) (err error) {
	e.logger.Info("extracting kafka metadata...")
	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return extractor.InvalidConfigError{}
	}
	err = e.validateConfig(config)
	if err != nil {
		return
	}
	client := NewClient(e.httpClient, config)
	uids, err := client.SearchAllDashboardUIDs()
	if err != nil {
		return
	}
	dashboardDetails, err := client.GetAllDashboardDetails(uids)
	if err != nil {
		return
	}
	data := make([]meteorMeta.Dashboard, len(dashboardDetails))
	for i, dashboardDetail := range dashboardDetails {
		data[i] = e.grafanaDashboardToMeteorDashboard(dashboardDetail)
	}

	out <- data
	return
}

func (e *Extractor) grafanaDashboardToMeteorDashboard(dashboard DashboardDetail) meteorMeta.Dashboard {
	charts := make([]*meteorMeta.Chart, len(dashboard.Dashboard.Panels))
	for i, panel := range dashboard.Dashboard.Panels {
		c := e.grafanaPanelToMeteorChart(panel, dashboard.Dashboard.UID, dashboard.Meta.URL)
		charts[i] = &c
	}
	return meteorMeta.Dashboard{
		Urn:         fmt.Sprintf("grafana.%s", dashboard.Dashboard.UID),
		Name:        dashboard.Meta.Slug,
		Source:      "grafana",
		Description: dashboard.Dashboard.Description,
		Url:         dashboard.Meta.URL,
		Charts:      charts,
	}
}

func (e *Extractor) grafanaPanelToMeteorChart(panel Panel, dashboardUID string, metaURL string) meteorMeta.Chart {
	var rawQuery string
	if len(panel.Targets) > 0 {
		rawQuery = panel.Targets[0].RawSQL
	}
	return meteorMeta.Chart{
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

func (e *Extractor) validateConfig(config Config) (err error) {
	if config.BaseURL == "" {
		return errors.New("base_url is required")
	}
	if config.APIKey == "" {
		return errors.New("api_key is required")
	}
	return
}

// Register the extractor to catalog
func init() {
	if err := extractor.Catalog.Register("grafana", &Extractor{
		logger: plugins.Log,
	}); err != nil {
		panic(err)
	}
}
