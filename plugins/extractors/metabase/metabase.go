package metabase

import (
	"bytes"
	"context"
	_ "embed" // used to print the embedded assets
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/models/odpf/assets/facets"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

var sampleConfig = `
host: http://localhost:3000
user_id: meteor_tester
password: meteor_pass_1234
label: my-metabase`

// Config hold the set of configuration for the metabase extractor
type Config struct {
	Host      string `mapstructure:"host" validate:"required"`
	Username  string `mapstructure:"username" validate:"required"`
	Password  string `mapstructure:"password" validate:"required"`
	Label     string `mapstructure:"label" validate:"required"`
	SessionID string `mapstructure:"session_id"`
}

// Extractor manages the extraction of data
// from the metabase server
type Extractor struct {
	config    Config
	sessionID string
	logger    log.Logger
	client    *http.Client
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}

// Info returns the brief information of the extractor
func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description:  "Dashboard list from Metabase server.",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"oss", "extractor"},
	}
}

// Validate validates the configuration of the extractor
func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

func (e *Extractor) Init(ctx context.Context, configMap map[string]interface{}) (err error) {
	// build and validate config
	err = utils.BuildConfig(configMap, &e.config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	e.client = &http.Client{
		Timeout: 4 * time.Second,
	}

	// get session id for further api calls in metabase
	if e.sessionID, err = e.getSessionID(); err != nil {
		return errors.Wrap(err, "failed to fetch session ID")
	}

	return nil
}

// Extract collects the metadata from the source. The metadata is collected through the out channel
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	dashboards, err := e.fetchDashboards()
	if err != nil {
		return errors.Wrap(err, "failed to fetch dashboard list")
	}
	for _, d := range dashboards {
		// we do not use "d" as the dashboard because it does not have
		// "ordered_cards" field
		dashboard, err := e.buildDashboard(d)
		if err != nil {
			return errors.Wrap(err, "failed to fetch dashboard data")
		}
		emit(models.NewRecord(dashboard))
	}
	return nil
}

func (e *Extractor) buildDashboard(d Dashboard) (data *assets.Dashboard, err error) {
	// we fetch dashboard again individually to get more fields
	dashboard, err := e.fetchDashboard(d.ID)
	if err != nil {
		err = errors.Wrapf(err, "error fetching dashboard")
		return
	}
	dashboardUrn := fmt.Sprintf("metabase::%s/dashboard/%d", e.config.Label, dashboard.ID)

	charts, err := e.buildCharts(dashboardUrn, dashboard)
	if err != nil {
		err = errors.Wrapf(err, "error building charts")
		return
	}

	createdAt, updatedAt, err := e.buildTimestamps(dashboard.BaseModel)
	if err != nil {
		err = errors.Wrapf(err, "error building dashboard timestamps")
		return
	}

	data = &assets.Dashboard{
		Resource: &common.Resource{
			Urn:     dashboardUrn,
			Name:    dashboard.Name,
			Service: "metabase",
		},
		Description: dashboard.Description,
		Charts:      charts,
		Properties: &facets.Properties{
			Attributes: utils.TryParseMapToProto(map[string]interface{}{
				"id":            dashboard.ID,
				"collection_id": dashboard.CollectionID,
				"creator_id":    dashboard.CreatorID,
			}),
		},
		Timestamps: &common.Timestamp{
			CreateTime: timestamppb.New(createdAt),
			UpdateTime: timestamppb.New(updatedAt),
		},
	}
	return
}

func (e *Extractor) buildCharts(dashboardUrn string, dashboard Dashboard) (charts []*assets.Chart, err error) {
	for _, oc := range dashboard.OrderedCards {
		card := oc.Card
		charts = append(charts, &assets.Chart{
			Urn:          fmt.Sprintf("metabase::%s/card/%d", e.config.Label, card.ID),
			DashboardUrn: dashboardUrn,
			Source:       "metabase",
			Properties: &facets.Properties{
				Attributes: utils.TryParseMapToProto(map[string]interface{}{
					"id":                     card.ID,
					"collection_id":          card.CollectionID,
					"creator_id":             card.CreatorID,
					"database_id":            card.DatabaseID,
					"table_id":               card.TableID,
					"query_average_duration": card.QueryAverageDuration,
					"display":                card.Display,
					"archived":               card.Archived,
				}),
			},
		})
	}

	return
}

func (e *Extractor) buildTimestamps(model BaseModel) (createdAt time.Time, updatedAt time.Time, err error) {
	createdAt, err = model.CreatedAt()
	if err != nil {
		err = errors.Wrap(err, "failed parsing created_at")
		return
	}
	updatedAt, err = model.UpdatedAt()
	if err != nil {
		err = errors.Wrap(err, "failed parsing updated_at")
		return
	}

	return
}

func (e *Extractor) fetchDashboard(dashboard_id int) (dashboard Dashboard, err error) {
	url := fmt.Sprintf("%s/api/dashboard/%d", e.config.Host, dashboard_id)
	err = e.makeRequest("GET", url, nil, &dashboard)

	return
}

func (e *Extractor) fetchDashboards() (data []Dashboard, err error) {
	url := fmt.Sprintf("%s/api/dashboard", e.config.Host)
	err = e.makeRequest("GET", url, nil, &data)

	return
}

func (e *Extractor) getSessionID() (sessionID string, err error) {
	if e.config.SessionID != "" {
		return e.config.SessionID, nil
	}

	payload := map[string]interface{}{
		"username": e.config.Username,
		"password": e.config.Password,
	}
	type responseID struct {
		ID string `json:"id"`
	}
	var data responseID
	err = e.makeRequest("POST", e.config.Host+"/api/session", payload, &data)
	if err != nil {
		return
	}
	return data.ID, nil
}

// helper function to avoid rewriting a request
func (e *Extractor) makeRequest(method, url string, payload interface{}, data interface{}) (err error) {
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return errors.Wrap(err, "failed to encode the payload JSON")
	}

	req, err := http.NewRequest(method, url, bytes.NewBuffer(jsonBytes))
	if err != nil {
		return errors.Wrap(err, "failed to create request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Metabase-Session", e.config.SessionID)

	res, err := e.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to generate response")
	}
	if res.StatusCode >= 300 {
		return fmt.Errorf("getting %d status code", res.StatusCode)
	}

	bytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return errors.Wrap(err, "failed to read response body")
	}
	if err = json.Unmarshal(bytes, &data); err != nil {
		return errors.Wrapf(err, "failed to parse: %s", string(bytes))
	}

	return
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("metabase", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
