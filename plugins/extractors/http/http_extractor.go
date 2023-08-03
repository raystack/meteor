package http

import (
	"context"
	_ "embed" // used to print the embedded assets
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/goto/meteor/metrics/otelhttpclient"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/salt/log"
)

var errUserExit = errors.New("user exit")

// init register the extractor to the catalog
func init() {
	if err := registry.Extractors.Register("http", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}

//go:embed README.md
var summary string

// Config holds the set of configuration for the HTTP extractor.
type Config struct {
	Request      RequestConfig `mapstructure:"request"`
	SuccessCodes []int         `mapstructure:"success_codes" validate:"dive,gte=200,lt=300" default:"[200]"`
	Concurrency  int           `mapstructure:"concurrency" validate:"gte=1,lte=100" default:"5"`
	Script       struct {
		Engine          string `mapstructure:"engine" validate:"required,oneof=tengo"`
		Source          string `mapstructure:"source" validate:"required"`
		MaxAllocs       int64  `mapstructure:"max_allocs" validate:"gt=100" default:"5000"`
		MaxConstObjects int    `mapstructure:"max_const_objects" validate:"gt=10" default:"500"`
	} `mapstructure:"script"`
}

type RequestConfig struct {
	RoutePattern string            `mapstructure:"route_pattern" default:""`
	URL          string            `mapstructure:"url" validate:"required,url"`
	QueryParams  []QueryParam      `mapstructure:"query_params" validate:"dive"`
	Method       string            `mapstructure:"method" validate:"oneof=GET POST" default:"GET"`
	Headers      map[string]string `mapstructure:"headers"`
	ContentType  string            `mapstructure:"content_type" validate:"required,oneof=application/json"`
	Accept       string            `mapstructure:"accept" validate:"required,oneof=application/json"`
	Body         interface{}       `mapstructure:"body"`
	Timeout      time.Duration     `mapstructure:"timeout" validate:"min=1ms" default:"5s"`
}

type QueryParam struct {
	Key   string `mapstructure:"key" validate:"required"`
	Value string `mapstructure:"value" validate:"required"`
}

var sampleConfig = heredoc.Doc(`
	request:
      route_pattern: /api/v1/endpoint 
	  url: "https://example.com/api/v1/endpoint"
	  method: "GET"
	  headers:
		"User-Id": "1a4336bc-bc6a-4972-83c1-d6426b4d79c3"
	  content_type: application/json
	  accept: application/json
	  timeout: 5s
	success_codes: [ 200 ]
	script:
	  engine: tengo
	  source: |
	    asset := new_asset("user")
	    // modify the asset using 'response'...
	    emit(asset)
`)

var info = plugins.Info{
	Description:  "Assets metadata from an external HTTP API",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"http", "extractor"},
}

// Extractor is responsible for executing an HTTP request as per configuration
// and executing the script with the response to 'extract' assets from within
// the script.
type Extractor struct {
	plugins.BaseExtractor

	logger         log.Logger
	config         Config
	executeRequest executeRequestFunc
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

	e.executeRequest = makeRequestExecutor(e.config.SuccessCodes, &http.Client{
		Transport: otelhttpclient.NewHTTPTransport(nil),
	})
	return nil
}

// Extract executes an HTTP request as per the configuration and if successful,
// executes the script. The script has access to the response and can use the
// same to 'emit' assets from within the script.
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	res, err := e.executeRequest(ctx, e.config.Request)
	if err != nil {
		return fmt.Errorf("http extractor: execute request: %w", err)
	}

	if err := e.executeScript(ctx, res, emit); err != nil {
		return fmt.Errorf("http extractor: execute script: %w", err)
	}

	return nil
}
