package metabase

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	"github.com/odpf/meteor/core"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins"
	meteorMeta "github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/utils"
)

var (
	client = &http.Client{
		Timeout: 2 * time.Second,
	}
	session_id = ""
)

type Config struct {
	UserID    string `mapstructure:"user_id" validate:"required"`
	Password  string `mapstructure:"password" validate:"required"`
	Host      string `mapstructure:"host" validate:"required"`
	SessionID string `mapstructure:"session_id"`
}

type responseID struct {
	ID string `json:"id"`
}

type Dashboard struct {
	ID          int `json:"id"`
	Urn         string
	Name        string  `json:"name"`
	Source      string  `default:"metabase"`
	Description string  `json:"description"`
	Charts      []Chart `json:"ordered_cards"`
}

type Chart struct {
	ID           int `json:"card_id"`
	Urn          string
	Source       string `default:"metabase"`
	DashboardUrn string
	DashboardID  int `json:"dashboard_id"`
}

type Extractor struct {
	out chan<- interface{}

	logger plugins.Logger
}

func New(logger plugins.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}

// Extract collects metdata from the source. Metadata is collected through the out channel
func (e *Extractor) Extract(ctx context.Context, config map[string]interface{}, out chan<- interface{}) (err error) {
	e.out = out
	// Build and validate config received from receipe
	var cfg Config
	if err := utils.BuildConfig(config, &cfg); err != nil {
		return extractor.InvalidConfigError{}
	}
	// get session id for further api calls in metabase
	if cfg.SessionID == "" {
		err = e.getSessionID(cfg)
		if err != nil {
			return
		}
	} else {
		session_id = cfg.SessionID
	}
	var dashboards []Dashboard
	err = e.getDashboardsList(cfg, &dashboards)
	if err != nil {
		return
	}
	for _, dashboard := range dashboards {
		dashboardInfo, err := e.getDashboardInfo(strconv.Itoa(dashboard.ID), cfg, dashboard.Name)
		if err != nil {
			return err
		}
		Charts := make([]*meteorMeta.Chart, len(dashboardInfo.Charts))
		for i, chart := range dashboardInfo.Charts {
			c := e.metabaseCardToMeteorChart(chart.Source, chart.DashboardUrn, chart.Urn)
			Charts[i] = &c
		}
		e.out <- meteorMeta.Dashboard{
			Urn:         fmt.Sprintf("metabase.%s", dashboard.Name),
			Name:        dashboard.Name,
			Source:      "metabase",
			Description: dashboard.Description,
			Charts:      Charts,
		}
	}
	return nil
}

func (e *Extractor) metabaseCardToMeteorChart(source, dashboardUrn, Urn string) meteorMeta.Chart {
	return meteorMeta.Chart{
		Urn:             Urn,
		Source:          source,
		DashboardUrn:    dashboardUrn,
		DashboardSource: "metabase",
	}
}

func (e *Extractor) getDashboardInfo(id string, cfg Config, name string) (data Dashboard, err error) {
	res, err := newRequest("GET", cfg.Host+"/api/dashboard/"+id, nil)
	if err != nil {
		return
	}
	err = unmarshalResponse(res, &data)
	if err != nil {
		return
	}
	var tempCards []Chart
	for _, card := range data.Charts {
		var tempCard Chart
		tempCard.Source = "metabase"
		tempCard.ID = card.ID
		tempCard.Urn = "metabase." + id + "." + strconv.Itoa(card.ID)
		tempCard.DashboardUrn = "metabase." + name
		tempCards = append(tempCards, tempCard)
	}
	data.Charts = tempCards
	return
}

func (e *Extractor) getSessionID(cfg Config) (err error) {
	values := map[string]interface{}{
		"username": cfg.UserID,
		"password": cfg.Password,
	}
	jsonValue, err := json.Marshal(values)
	if err != nil {
		return
	}
	res, err := newRequest("POST", cfg.Host+"/api/session", bytes.NewBuffer(jsonValue))
	if err != nil {
		return
	}
	var data responseID
	err = unmarshalResponse(res, &data)
	if err != nil {
		return
	}
	session_id = data.ID
	return
}

func (e *Extractor) getDashboardsList(cfg Config, data interface{}) (err error) {
	res, err := newRequest("GET", cfg.Host+"/api/dashboard/", nil)
	if err != nil {
		return
	}
	err = unmarshalResponse(res, &data)
	if err != nil {
		return
	}
	return
}

// helper function to avoid rewriting a request
func newRequest(method, url string, body io.Reader) (res *http.Response, err error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if session_id != "" {
		req.Header.Set("X-Metabase-Session", session_id)
	}
	res, err = client.Do(req)
	if err != nil {
		return
	}
	return
}

// Converts http response to map
func unmarshalResponse(res *http.Response, data interface{}) (err error) {
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}
	err = json.Unmarshal(b, &data)
	return
}

// Registers the extractor to catalog
func init() {
	if err := extractor.Catalog.Register("metabase", func() core.Extractor {
		return New(plugins.Log)
	}); err != nil {
		panic(err)
	}
}
