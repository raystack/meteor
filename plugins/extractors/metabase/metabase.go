package metabase

import (
	"bytes"
	"context"
	_ "embed" // used to print the embedded assets
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/models/odpf/assets/common"
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
 password: meteor_pass_1234`

// Config hold the set of configuration for the metabase extractor
type Config struct {
	UserID    string `mapstructure:"user_id" validate:"required"`
	Password  string `mapstructure:"password" validate:"required"`
	Host      string `mapstructure:"host" validate:"required"`
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
		Tags:         []string{"oss,extractor"},
	}
}

// Validate validates the configuration of the extractor
func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

func (e *Extractor) Init(ctx context.Context, configMap map[string]interface{}) (err error) {
	// build and validateconfig
	err = utils.BuildConfig(configMap, &e.config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	e.client = &http.Client{
		Timeout: 4 * time.Second,
	}

	// get session id for further api calls in metabase
	e.sessionID, err = e.getSessionID()
	if err != nil {
		return
	}

	return nil
}

// Extract collects the metadata from the source. The metadata is collected through the out channel
func (e *Extractor) Extract(ctx context.Context, emitter plugins.Emitter) (err error) {
	dashboards, err := e.getDashboardsList()
	if err != nil {
		return
	}
	for _, dashboard := range dashboards {
		data, err := e.buildDashboard(strconv.Itoa(dashboard.ID), dashboard.Name)
		if err != nil {
			return err
		}
		emitter.Emit(models.NewRecord(data))
	}
	return nil
}

func (e *Extractor) buildDashboard(id string, name string) (data *assets.Dashboard, err error) {
	var dashboard Dashboard
	err = e.makeRequest("GET", e.config.Host+"/api/dashboard/"+id, nil, &dashboard)
	if err != nil {
		return
	}
	var tempCards []*assets.Chart
	for _, card := range dashboard.Charts {
		var tempCard assets.Chart
		tempCard.Source = "metabase"
		tempCard.Urn = "metabase." + id + "." + strconv.Itoa(card.ID)
		tempCard.DashboardUrn = "metabase." + name
		tempCards = append(tempCards, &tempCard)
	}
	data = &assets.Dashboard{
		Resource: &common.Resource{
			Urn:     fmt.Sprintf("metabase.%s", dashboard.Name),
			Name:    dashboard.Name,
			Service: "metabase",
		},
		Description: dashboard.Description,
		Charts:      tempCards,
	}
	return
}

func (e *Extractor) getDashboardsList() (data []Dashboard, err error) {
	err = e.makeRequest("GET", e.config.Host+"/api/dashboard/", nil, &data)
	if err != nil {
		return
	}
	return
}

func (e *Extractor) getSessionID() (sessionID string, err error) {
	if e.config.SessionID != "" {
		return e.config.SessionID, nil
	}

	payload := map[string]interface{}{
		"username": e.config.UserID,
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
	jsonifyPayload, err := json.Marshal(payload)
	if err != nil {
		return
	}
	body := bytes.NewBuffer(jsonifyPayload)
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if e.config.SessionID != "" {
		req.Header.Set("X-Metabase-Session", e.config.SessionID)
	}
	res, err := e.client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}
	err = json.Unmarshal(b, &data)
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
