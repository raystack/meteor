package redash

import (
	"bytes"
	"context"
	_ "embed" // used to print the embedded assets
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

// Config holds the set of configuration for the redash extractor
type Config struct {
	BaseURL string `json:"base_url" yaml:"base_url" mapstructure:"base_url" validate:"required"`
	ApiKey  string `json:"api_key" yaml:"api_key" mapstructure:"api_key" validate:"required"`
}

var sampleConfig = `
# Each endpoint is appended to your Redash base URL
base_url: https://redash.example.com
# Redash API calls support authentication with an API key.
api_key: t33I8i8OFnVt3t9Bjj2RXr8nCBz0xyzVZ318Zwbj
`

var info = plugins.Info{
	Description:  "Dashboard list from Redash server.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "extractor"},
}

// Extractor manages the extraction of data from the redash server
type Extractor struct {
	plugins.BaseExtractor
	config Config
	logger log.Logger
	client *http.Client
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
	e.client = &http.Client{
		Timeout: 4 * time.Second,
	}

	return
}

// Extract collects metadata of each dashboard through emitter
func (e *Extractor) Extract(_ context.Context, emit plugins.Emit) (err error) {
	dashboards, err := e.getDashboardsList()
	if err != nil {
		return fmt.Errorf("failed to get dashboard list: %w", err)
	}

	for _, dashboard := range dashboards {
		data, err := e.buildDashboard(dashboard)
		if err != nil {
			return fmt.Errorf("failed to build dashboard: %w", err)
		}
		emit(models.NewRecord(data))
	}

	return
}

// buildDashboard builds a dashboard from redash server
func (e *Extractor) buildDashboard(dashboard Results) (asset *v1beta2.Asset, err error) {
	dashboardUrn := models.NewURN("redash", e.UrnScope, "dashboard", fmt.Sprintf("%d", dashboard.Id))

	data, err := anypb.New(&v1beta2.Dashboard{
		Attributes: utils.TryParseMapToProto(map[string]interface{}{
			"user_id": dashboard.UserId,
			"version": dashboard.Version,
			"slug":    dashboard.Slug,
		}),
	})
	if err != nil {
		err = fmt.Errorf("error creating Any struct: %w", err)
		return nil, err
	}
	asset = &v1beta2.Asset{
		Urn:     dashboardUrn,
		Name:    dashboard.Name,
		Service: "redash",
		Type:    "dashboard",
		Data:    data,
	}

	return
}

// getDashboardsList gets a list of dashboards from redash server
func (e *Extractor) getDashboardsList() (dashboards []Results, err error) {
	type response struct {
		Count    int       `json:"count"`
		Page     int       `json:"page"`
		PageSize int       `json:"page_size"`
		Results  []Results `json:"results"`
	}

	var data response
	if err = e.makeRequest("GET",
		fmt.Sprintf("%s/api/dashboards", e.config.BaseURL), nil, &data); err != nil {
		err = fmt.Errorf("failed to get dashboard: %w", err)
		return
	}

	return data.Results, nil
}

// makeRequest helper function to avoid rewriting a request
func (e *Extractor) makeRequest(method, url string, payload interface{}, data interface{}) (err error) {
	jsonifyPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to encode the payload JSON: %w", err)
	}
	body := bytes.NewBuffer(jsonifyPayload)
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	var key = e.config.ApiKey
	req.Header.Add("Content-Type", "application/json")
	req.Header.Set("Authorization", key)
	req.Header.Set("Referer", url)

	res, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to generate response: %w", err)
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("response failed with status code %d: %w", res.StatusCode, err)
	}
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}
	if err = json.Unmarshal(b, &data); err != nil {
		return fmt.Errorf("failed to parse %s: %w", string(b), err)
	}

	return
}

// init register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("redash", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
