package http

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/MakeNowJust/heredoc"
	"github.com/goto/meteor/models"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/salt/log"
)

//go:embed README.md
var summary string

type Config struct {
	URL         string            `mapstructure:"url" validate:"required"`
	Headers     map[string]string `mapstructure:"headers"`
	Method      string            `mapstructure:"method" validate:"required"`
	SuccessCode int               `mapstructure:"success_code" default:"200"`
}

var info = plugins.Info{
	Description: "Send metadata to http service",
	Summary:     summary,
	Tags:        []string{"http", "sink"},
	SampleConfig: heredoc.Doc(`
	# The url (hostname and route) of the http service
	url: https://compass.com/route
	method: "PUT"
	# Additional HTTP headers, multiple headers value are separated by a comma
	headers:
	  X-Other-Header: value1, value2
	`),
}

type httpClient interface {
	Do(*http.Request) (*http.Response, error)
}

type Sink struct {
	plugins.BasePlugin
	client httpClient
	config Config
	logger log.Logger
}

func New(c httpClient, logger log.Logger) plugins.Syncer {
	s := &Sink{
		logger: logger,
		client: c,
	}
	s.BasePlugin = plugins.NewBasePlugin(info, &s.config)

	return s
}

func (s *Sink) Init(ctx context.Context, config plugins.Config) error {
	return s.BasePlugin.Init(ctx, config)
}

func (s *Sink) Sink(ctx context.Context, batch []models.Record) error {
	for _, record := range batch {
		metadata := record.Data()
		s.logger.Info("sinking record to http", "record", metadata.Urn)
		payload, err := json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("build http payload: %w", err)
		}

		if err = s.send(ctx, payload); err != nil {
			return fmt.Errorf("send data: %w", err)
		}

		s.logger.Info("successfully sinked record to http", "record", metadata.Urn)
	}

	return nil
}

func (*Sink) Close() error { return nil }

func (s *Sink) send(ctx context.Context, payloadBytes []byte) error {
	// send request
	req, err := http.NewRequestWithContext(ctx, s.config.Method, s.config.URL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return err
	}

	for hdrKey, hdrVal := range s.config.Headers {
		hdrVals := strings.Split(hdrVal, ",")
		for _, val := range hdrVals {
			req.Header.Add(hdrKey, val)
		}
	}

	res, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer plugins.DrainBody(res)

	if res.StatusCode == s.config.SuccessCode {
		return nil
	}

	var bodyBytes []byte
	bodyBytes, err = io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	err = fmt.Errorf("http returns %d: %v", res.StatusCode, string(bodyBytes))

	switch code := res.StatusCode; {
	case code >= 500:
		return plugins.NewRetryError(err)
	default:
		return err
	}
}

func init() {
	if err := registry.Sinks.Register("http", func() plugins.Syncer {
		return New(&http.Client{}, plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
