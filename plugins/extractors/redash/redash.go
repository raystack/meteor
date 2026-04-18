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

	"github.com/raystack/meteor/metrics/otelhttpclient"
	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/internal/urlbuilder"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
)

//go:embed README.md
var summary string

// Config holds the set of configuration for the redash extractor
type Config struct {
	BaseURL string `mapstructure:"base_url" validate:"required"`
	ApiKey  string `mapstructure:"api_key" validate:"required"`
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
	urlb   urlbuilder.Source
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

	e.client = &http.Client{
		Timeout:   4 * time.Second,
		Transport: otelhttpclient.NewHTTPTransport(nil),
	}

	urlb, err := urlbuilder.NewSource(e.config.BaseURL)
	if err != nil {
		return err
	}
	e.urlb = urlb

	return nil
}

// Extract collects metadata of each dashboard through emitter
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	dashboards, err := e.getDashboardsList(ctx)
	if err != nil {
		return fmt.Errorf("get dashboard list: %w", err)
	}

	for _, dashboard := range dashboards {
		record := e.buildDashboard(dashboard)
		emit(record)
	}

	return nil
}

// buildDashboard builds a dashboard record from redash server
func (e *Extractor) buildDashboard(dashboard Results) models.Record {
	dashboardUrn := models.NewURN("redash", e.UrnScope, "dashboard", fmt.Sprintf("%d", dashboard.Id))

	props := map[string]any{
		"user_id":     dashboard.UserId,
		"version":     dashboard.Version,
		"slug":        dashboard.Slug,
		"is_archived": dashboard.IsArchived,
		"is_draft":    dashboard.IsDraft,
	}
	if len(dashboard.Tags) > 0 {
		props["tags"] = dashboard.Tags
	}
	if !dashboard.CreatedAt.IsZero() {
		props["created_at"] = dashboard.CreatedAt.Format(time.RFC3339)
	}
	if !dashboard.UpdatedAt.IsZero() {
		props["updated_at"] = dashboard.UpdatedAt.Format(time.RFC3339)
	}

	entity := models.NewEntity(dashboardUrn, "dashboard", dashboard.Name, "redash", props)

	// Create owned_by edge to the dashboard creator.
	var edges []*meteorv1beta1.Edge
	if dashboard.User.Id != 0 {
		ownerURN := models.NewURN("redash", e.UrnScope, "user", fmt.Sprintf("%d", dashboard.User.Id))
		edges = append(edges, models.OwnerEdge(dashboardUrn, ownerURN, "redash"))
	}

	return models.NewRecord(entity, edges...)
}

// getDashboardsList gets a list of dashboards from redash server
func (e *Extractor) getDashboardsList(ctx context.Context) ([]Results, error) {
	const listDashboardsRoute = "/api/dashboards"
	targetURL := e.urlb.New().Path(listDashboardsRoute).URL()

	var data struct {
		Count    int       `json:"count"`
		Page     int       `json:"page"`
		PageSize int       `json:"page_size"`
		Results  []Results `json:"results"`
	}
	if err := e.makeRequest(ctx, listDashboardsRoute, http.MethodGet, targetURL.String(), nil, &data); err != nil {
		return nil, fmt.Errorf("get dashboard: %w", err)
	}

	return data.Results, nil
}

// makeRequest helper function to avoid rewriting a request
//
//nolint:revive
func (e *Extractor) makeRequest(ctx context.Context, route, method, url string, payload, result any) error {
	jsonifyPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode the payload JSON: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewBuffer(jsonifyPayload))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Set("Authorization", e.config.ApiKey)
	req.Header.Set("Referer", url)
	req = otelhttpclient.AnnotateRequest(req, route)

	res, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer plugins.DrainBody(res)

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("response status code %d", res.StatusCode)
	}

	b, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}
	if err = json.Unmarshal(b, &result); err != nil {
		return fmt.Errorf("parse %s: %w", string(b), err)
	}

	return nil
}

// init register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("redash", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
