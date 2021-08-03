package metabase

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
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
		Timeout: 10 * time.Second,
	}
	session_id = ""
)

type Config struct {
	UserID   string `mapstructure:"user_id" validate:"required"`
	Password string `mapstructure:"password" validate:"required"`
	Host     string `mapstructure:"host" validate:"required"`
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
	err = e.getSessionID(cfg)
	if err != nil {
		return
	}
	dashboards, err := e.getDashboards(cfg)
	if err != nil {
		return
	}
	// data := make([]meteorMeta.Dashboard, len(dashboards))
	for _, dashboard := range dashboards {
		e.out <- meteorMeta.Dashboard{
			Urn:         fmt.Sprintf("metabase.%s", dashboard),
			Name:        dashboard["name"].(string),
			Source:      "metabase",
			Description: dashboard["description"].(string),
		}
	}
	return nil
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
	body, err := unmarshalResponse(res)
	if err != nil {
		return
	}
	session_id = body["id"].(string)
	return
}

func (e *Extractor) getDashboards(cfg Config) (body []map[string]interface{}, err error) {
	res, err := newRequest("GET", cfg.Host+"/api/dashboard", nil)
	if err != nil {
		return
	}
	body, err = unmarshalArray(res)
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
func unmarshalResponse(res *http.Response) (data map[string]interface{}, err error) {
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}
	err = json.Unmarshal(b, &data)
	return
}

func unmarshalArray(res *http.Response) (data []map[string]interface{}, err error) {
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}
	err = json.Unmarshal(b, &data)
	return
}

// Registers the extractor to catalog
func init() {
	if err := extractor.Catalog.Register("mysql", func() core.Extractor {
		return New(plugins.Log)
	}); err != nil {
		panic(err)
	}
}
