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

	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/salt/log"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

//go:embed README.md
var summary string

// Config holds the set of configuration for the superset extractor
type Config struct {
	Username string `mapstructure:"username" validate:"required"`
	Password string `mapstructure:"password" validate:"required"`
	Host     string `mapstructure:"host" validate:"required"`
	Provider string `mapstructure:"provider" validate:"required"`
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
		Timeout: 4 * time.Second,
	}
	// get access token for further api calls in superset
	var err error
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
		data, err := e.buildDashboard(ctx, dashboard.ID)
		if err != nil {
			return fmt.Errorf("build dashboard: %w", err)
		}
		emit(models.NewRecord(data))
	}
	return nil
}

// buildDashboard builds a dashboard from superset server
func (e *Extractor) buildDashboard(ctx context.Context, id int) (*v1beta2.Asset, error) {
	var dashboard Dashboard
	chart, err := e.getChartsList(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("fetch charts: %w", err)
	}

	data, err := anypb.New(&v1beta2.Dashboard{
		Charts:     chart,
		Attributes: &structpb.Struct{}, // ensure attributes don't get overwritten if present
	})
	if err != nil {
		return nil, err
	}

	return &v1beta2.Asset{
		Urn:     models.NewURN("superset", e.UrnScope, "dashboard", fmt.Sprintf("%d", dashboard.ID)),
		Name:    dashboard.DashboardTitle,
		Service: "superset",
		Url:     dashboard.URL,
		Type:    "dashboard",
		Data:    data,
	}, nil
}

// getDashboardsList gets a list of dashboards from superset server
func (e *Extractor) getDashboardsList(ctx context.Context) ([]Dashboard, error) {
	var data struct {
		Result []Dashboard `json:"result"`
	}
	if err := e.makeRequest(ctx, "GET", e.config.Host+"/api/v1/dashboard", nil, &data); err != nil {
		return nil, fmt.Errorf("get dashboard: %w", err)
	}
	return data.Result, nil
}

// getChartsList gets a list of charts from superset server
func (e *Extractor) getChartsList(ctx context.Context, id int) ([]*v1beta2.Chart, error) {
	var data struct {
		Result []Chart `json:"result"`
	}
	if err := e.makeRequest(ctx, "GET", fmt.Sprintf("%s/api/v1/dashboard/%d/charts", e.config.Host, id), nil, &data); err != nil {
		return nil, fmt.Errorf("fetch chart details: %w", err)
	}
	var charts []*v1beta2.Chart
	for _, res := range data.Result {
		charts = append(charts, &v1beta2.Chart{
			Urn:          models.NewURN("superset", e.UrnScope, "chart", fmt.Sprintf("%d", res.SliceId)),
			Name:         res.SliceName,
			Source:       "superset",
			Description:  res.Description,
			Url:          res.SliceUrl,
			DataSource:   res.Datasource,
			DashboardUrn: "dashboard:" + strconv.Itoa(id),
		})
	}
	return charts, nil
}

// getAccessToken authenticate and get a JWT access token
func (e *Extractor) getAccessToken(ctx context.Context) (string, error) {
	payload := map[string]interface{}{
		"username": e.config.Username,
		"password": e.config.Password,
		"provider": e.config.Provider,
	}
	var data struct {
		Token string `json:"access_token"`
	}
	if err := e.makeRequest(ctx, "POST", e.config.Host+"/api/v1/security/login", payload, &data); err != nil {
		return "", fmt.Errorf("fetch access token: %w", err)
	}
	return data.Token, nil
}

// getCsrfToken fetch the CSRF token
func (e *Extractor) getCsrfToken(ctx context.Context) (string, error) {
	var data struct {
		CsrfToken string `json:"result"`
	}
	if err := e.makeRequest(ctx, "GET", e.config.Host+"/api/v1/security/csrf_token/", nil, &data); err != nil {
		return "", fmt.Errorf("fetch csrf token: %w", err)
	}
	return data.CsrfToken, nil
}

// makeRequest helper function to avoid rewriting a request
func (e *Extractor) makeRequest(ctx context.Context, method, url string, payload, result interface{}) error {
	jsonifyPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode the payload JSON: %w", err)
	}
	body := bytes.NewBuffer(jsonifyPayload)
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	bearer := "Bearer " + e.accessToken
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", bearer)
	req.Header.Set("X-CSRFToken", e.csrfToken)
	req.Header.Set("Referer", url)

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
