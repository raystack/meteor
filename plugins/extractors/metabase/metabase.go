package metabase

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	// _ "github.com/lib/pq"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

var (
	client = &http.Client{
		Timeout: 4 * time.Second,
	}
)

type Config struct {
	UserID    string `mapstructure:"user_id" validate:"required"`
	Password  string `mapstructure:"password" validate:"required"`
	Url       string `mapstructure:"url" validate:"required"`
	SessionID string `mapstructure:"session_id"`
}

type Extractor struct {
	cfg       Config
	sessionID string
	logger    log.Logger
}

// Extract collects metdata from the source. Metadata is collected through the out channel
func (e *Extractor) Extract(ctx context.Context, configMap map[string]interface{}, out chan<- interface{}) (err error) {
	// build and validateconfig
	err = utils.BuildConfig(configMap, &e.cfg)
	if err != nil {
		return plugins.InvalidConfigError{}
	}
	// get session id for further api calls in metabase
	e.sessionID, err = e.getSessionID()
	if err != nil {
		return
	}

	dashboards, err := e.getDashboardsList()
	if err != nil {
		return
	}
	for _, dashboard := range dashboards {
		data, err := e.buildDashboard(strconv.Itoa(dashboard.ID), dashboard.Name)
		if err != nil {
			return err
		}
		out <- data
	}
	return nil
}

func (e *Extractor) buildDashboard(id string, name string) (data meta.Dashboard, err error) {
	var dashboard Dashboard
	err = e.makeRequest("GET", e.cfg.Url+"/api/dashboard/"+id, nil, &dashboard)
	if err != nil {
		return
	}
	var tempCards []*meta.Chart
	for _, card := range dashboard.Charts {
		var tempCard meta.Chart
		tempCard.Source = "metabase"
		tempCard.Urn = "metabase." + id + "." + strconv.Itoa(card.ID)
		tempCard.DashboardUrn = "metabase." + name
		tempCards = append(tempCards, &tempCard)
	}
	data = meta.Dashboard{
		Urn:         fmt.Sprintf("metabase.%s", dashboard.Name),
		Name:        dashboard.Name,
		Source:      "metabase",
		Description: dashboard.Description,
		Charts:      tempCards,
	}
	return
}

func (e *Extractor) getDashboardsList() (data []Dashboard, err error) {
	err = e.makeRequest("GET", e.cfg.Url+"/api/dashboard/", nil, &data)
	if err != nil {
		return
	}
	return
}

func (e *Extractor) getSessionID() (sessionID string, err error) {
	if e.cfg.SessionID != "" {
		return e.cfg.SessionID, nil
	}

	payload := map[string]interface{}{
		"username": e.cfg.UserID,
		"password": e.cfg.Password,
	}
	type responseID struct {
		ID string `json:"id"`
	}
	var data responseID
	err = e.makeRequest("POST", e.cfg.Url+"/api/session", payload, &data)
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
	if e.cfg.SessionID != "" {
		req.Header.Set("X-Metabase-Session", e.cfg.SessionID)
	}
	res, err := client.Do(req)
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

func New(logger log.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("metabase", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
