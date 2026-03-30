package superset

import (
	"bytes"
	"context"
	_ "embed" // used to print the embedded assets
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/raystack/meteor/metrics/otelhttpclient"
	"github.com/raystack/meteor/models"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/internal/urlbuilder"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
)

//go:embed README.md
var summary string

// Config holds the set of configuration for the superset extractor
type Config struct {
	Username string `json:"username" yaml:"username" mapstructure:"username" validate:"required"`
	Password string `json:"password" yaml:"password" mapstructure:"password" validate:"required"`
	Host     string `json:"host" yaml:"host" mapstructure:"host" validate:"required"`
	Provider string `json:"provider" yaml:"provider" mapstructure:"provider" validate:"required"`
}

var sampleConfig = `
username: meteor_tester
password: meteor_pass_1234
host: http://localhost:3000
provider: db`

var info = plugins.Info{
	Description:  "Dashboard list from Superset server.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "extractor"},
}

// Extractor manages the extraction of data
// from the superset server
type Extractor struct {
	plugins.BaseExtractor
	config      Config
	accessToken string
	csrfToken   string
	logger      log.Logger
	client      *http.Client
	urlb        urlbuilder.Source
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

	urlb, err := urlbuilder.NewSource(e.config.Host)
	if err != nil {
		return err
	}
	e.urlb = urlb

	// get access token for further api calls in superset
	e.accessToken, err = e.getAccessToken(ctx)
	if err != nil {
		return fmt.Errorf("get access token: %w", err)
	}

	// get csrf token for secure api calls in superset
	e.csrfToken, err = e.getCsrfToken(ctx)
	if err != nil {
		return fmt.Errorf("get csrf token: %w", err)
	}

	return nil
}

// Extract collects metadata of each dashboard through emitter
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	dashboards, err := e.getDashboardsList(ctx)
	if err != nil {
		return fmt.Errorf("get dashboards: %w", err)
	}
	for _, dashboard := range dashboards {
		record, err := e.buildDashboard(ctx, dashboard.ID)
		if err != nil {
			return fmt.Errorf("build dashboard: %w", err)
		}
		emit(record)
	}
	return nil
}

// buildDashboard builds a dashboard from superset server
func (e *Extractor) buildDashboard(ctx context.Context, id int) (models.Record, error) {
	var dashboard Dashboard
	charts, err := e.getChartsList(ctx, id)
	if err != nil {
		return models.Record{}, fmt.Errorf("fetch charts: %w", err)
	}

	urn := models.NewURN("superset", e.UrnScope, "dashboard", fmt.Sprintf("%d", id))
	props := map[string]any{
		"charts": charts,
	}
	if dashboard.URL != "" {
		props["url"] = dashboard.URL
	}

	entity := models.NewEntity(urn, "dashboard", dashboard.DashboardTitle, "superset", props)
	return models.NewRecord(entity), nil
}

// getDashboardsList gets a list of dashboards from superset server
func (e *Extractor) getDashboardsList(ctx context.Context) ([]Dashboard, error) {
	var data struct {
		Result []Dashboard `json:"result"`
	}

	const listDashboardRoute = "/api/v1/dashboard"
	targetURL := e.urlb.New().Path(listDashboardRoute).URL()
	if err := e.makeRequest(ctx, listDashboardRoute, http.MethodGet, targetURL.String(), nil, &data); err != nil {
		return nil, fmt.Errorf("get dashboard: %w", err)
	}
	return data.Result, nil
}

// getChartsList gets a list of charts from superset server
func (e *Extractor) getChartsList(ctx context.Context, id int) ([]map[string]any, error) {
	const listChartsRoute = "/api/v1/dashboard/{id}/charts"
	targetURL := e.urlb.New().Path(listChartsRoute).PathParamInt("id", int64(id)).URL()

	var data struct {
		Result []Chart `json:"result"`
	}
	if err := e.makeRequest(ctx, listChartsRoute, http.MethodGet, targetURL.String(), nil, &data); err != nil {
		return nil, fmt.Errorf("fetch chart details: %w", err)
	}

	var charts []map[string]any
	for _, res := range data.Result {
		chart := map[string]any{
			"urn":           models.NewURN("superset", e.UrnScope, "chart", fmt.Sprintf("%d", res.SliceId)),
			"name":          res.SliceName,
			"source":        "superset",
			"url":           res.SliceUrl,
			"data_source":   res.Datasource,
			"dashboard_urn": "dashboard:" + strconv.Itoa(id),
		}
		if res.Description != "" {
			chart["description"] = res.Description
		}
		charts = append(charts, chart)
	}
	return charts, nil
}

// getAccessToken authenticate and get a JWT access token
func (e *Extractor) getAccessToken(ctx context.Context) (string, error) {
	const loginRoute = "/api/v1/security/login"
	targetURL := e.urlb.New().Path(loginRoute).URL()

	payload := map[string]any{
		"username": e.config.Username,
		"password": e.config.Password,
		"provider": e.config.Provider,
	}
	var data struct {
		Token string `json:"access_token"`
	}
	if err := e.makeRequest(ctx, loginRoute, http.MethodPost, targetURL.String(), payload, &data); err != nil {
		return "", fmt.Errorf("fetch access token: %w", err)
	}
	return data.Token, nil
}

// getCsrfToken fetch the CSRF token
func (e *Extractor) getCsrfToken(ctx context.Context) (string, error) {
	const csrfTokenRoute = "/api/v1/security/csrf_token/"
	targetURL := e.urlb.New().Path(csrfTokenRoute).URL()

	var data struct {
		CsrfToken string `json:"result"`
	}
	if err := e.makeRequest(ctx, csrfTokenRoute, http.MethodGet, targetURL.String(), nil, &data); err != nil {
		return "", fmt.Errorf("fetch csrf token: %w", err)
	}
	return data.CsrfToken, nil
}

// makeRequest helper function to avoid rewriting a request
//
//nolint:revive
func (e *Extractor) makeRequest(ctx context.Context, route, method, url string, payload, result any) error {
	jsonifyPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode the payload JSON: %w", err)
	}
	body := bytes.NewBuffer(jsonifyPayload)
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+e.accessToken)
	req.Header.Set("X-CSRFToken", e.csrfToken)
	req.Header.Set("Referer", url)
	req = otelhttpclient.AnnotateRequest(req, route)

	res, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("generate response: %w", err)
	}
	defer plugins.DrainBody(res)

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("response failed with status code: %d", res.StatusCode)
	}

	b, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}

	if err = json.Unmarshal(b, &result); err != nil {
		return fmt.Errorf("parse: %s: %w", string(b), err)
	}

	return nil
}

// init register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("superset", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
