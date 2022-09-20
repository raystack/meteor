package github

import (
	"bytes"
	"context"
	_ "embed" // used to print the embedded assets
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

type Request struct {
	URL     string                 `mapstructure:"url" validate:"required"`
	Path    string                 `mapstructure:"path"`
	Headers map[string]string      `mapstructure:"headers"`
	Method  string                 `mapstructure:"method" validate:"required"`
	Query   map[string]string      `mapstructure:"query"`
	Body    map[string]interface{} `mapstructure:"body"`
}

type Response struct {
}

// Config holds the set of configuration for the extractor
type Config struct {
	Request  Request  `mapstructure:"request" validate:"required"`
	Response Response `mapstructure:"response" validate:"required"`
}

var info = plugins.Info{
	Description: "Extract metadata from http service",
	Summary:     summary,
	Tags:        []string{"http", "extractor"},
}

type httpClient interface {
	Do(*http.Request) (*http.Response, error)
}

// Extractor manages the extraction of data from the extractor
type Extractor struct {
	plugins.BaseExtractor
	logger log.Logger
	config Config
	client httpClient
}

// New returns a pointer to an initialized Extractor Object
func New(c httpClient, logger log.Logger) *Extractor {
	e := &Extractor{
		logger: logger,
		client: c,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	return e
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	return
}

// Extract extracts the data from the extractor
// The data is returned as a list of assets.Asset
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	req, err := e.buildHTTPRequest(ctx)
	if err != nil {
		return fmt.Errorf("failed to build HTTP request: %v", err)
	}

	res, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to extract: %v", err)
	}

	if res.StatusCode == 200 {
		var bodyBytes []byte
		bodyBytes, err = ioutil.ReadAll(res.Body)
		if err != nil {
			return fmt.Errorf("failed to extract body: %v", err)
		}
		e.logger.Info("http returns %d: %v", res.StatusCode, string(bodyBytes))

		return nil
	}

	return fmt.Errorf("request failed with status: %v", res.StatusCode)
}

func (e *Extractor) buildHTTPRequest(ctx context.Context) (*http.Request, error) {
	var URL string

	if e.config.Request.Path != "" {
		URL = e.config.Request.URL + "/" + e.config.Request.Path
	}

	params := url.Values{}
	if e.config.Request.Query != nil {
		for param, value := range e.config.Request.Query {
			values := strings.Split(value, ",")

			valueArr := []string{}
			for _, val := range values {
				valueArr = append(valueArr, val)
			}
			params[param] = valueArr
		}
		URL = URL + "?" + params.Encode()
	}

	payloadBytes, err := json.Marshal(e.config.Request.Body)
	if err != nil {
		e.logger.Error("Unable to marshal body", err)
	}

	req, err := http.NewRequestWithContext(ctx, e.config.Request.Method, URL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return nil, err
	}

	for hdrKey, hdrVal := range e.config.Request.Headers {
		hdrVals := strings.Split(hdrVal, ",")
		for _, val := range hdrVals {
			req.Header.Add(hdrKey, val)
		}
	}

	return req, nil
}

// init registers the extractor to catalog
func init() {
	if err := registry.Extractors.Register("http", func() plugins.Extractor {
		return New(&http.Client{}, plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
