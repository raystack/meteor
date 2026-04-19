package compass

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
	"github.com/raystack/meteor/metrics/otelhttpclient"
	"github.com/raystack/meteor/models"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/internal/urlbuilder"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
	"golang.org/x/sync/errgroup"
)

//go:embed README.md
var summary string

type Config struct {
	Host           string            `json:"host" yaml:"host" mapstructure:"host" validate:"required,url"`
	Headers        map[string]string `json:"headers" yaml:"headers" mapstructure:"headers"`
	MaxConcurrency int               `json:"max_concurrency" yaml:"max_concurrency" mapstructure:"max_concurrency"`
}

var info = plugins.Info{
	Description: "Send metadata to compass http service",
	Summary:     summary,
	Tags:        []string{"oss", "catalog"},
	SampleConfig: heredoc.Doc(`
	# The hostname of the compass service
	host: https://compass.com
	# Additional HTTP headers send to compass, multiple headers value are separated by a comma
	headers:
	  Compass-User-UUID: meteor@raystack.io
	  X-Other-Header: value1, value2
	# Maximum number of concurrent requests to compass per batch. 0 means no limit.
	# max_concurrency: 10
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
	urlb   urlbuilder.Source
}

func New(c httpClient, logger log.Logger) plugins.Syncer {
	if cl, ok := c.(*http.Client); ok {
		cl.Transport = otelhttpclient.NewHTTPTransport(cl.Transport)
	}

	s := &Sink{client: c, logger: logger}
	s.BasePlugin = plugins.NewBasePlugin(info, &s.config)
	return s
}

func (s *Sink) Init(ctx context.Context, config plugins.Config) error {
	if err := s.BasePlugin.Init(ctx, config); err != nil {
		return err
	}

	urlb, err := urlbuilder.NewSource(s.config.Host)
	if err != nil {
		return err
	}
	s.urlb = urlb

	return nil
}

func (s *Sink) Sink(ctx context.Context, batch []models.Record) error {
	if len(batch) == 0 {
		return nil
	}

	errGroup := errgroup.Group{}
	limit := len(batch)
	if s.config.MaxConcurrency > 0 && s.config.MaxConcurrency < limit {
		limit = s.config.MaxConcurrency
	}
	errGroup.SetLimit(limit)

	for _, record := range batch {
		record := record
		errGroup.Go(func() error {
			entity := record.Entity()
			s.logger.Info("sinking record to compass", "record", entity.GetUrn())

			if err := s.sinkRecord(ctx, record); err != nil {
				return fmt.Errorf("sink entity %s: %w", entity.GetUrn(), err)
			}

			s.logger.Info("successfully sinked record to compass", "record", entity.GetUrn())
			return nil
		})
	}

	return errGroup.Wait()
}

func (*Sink) Close() error { return nil }

func (s *Sink) sinkRecord(ctx context.Context, record models.Record) error {
	entity := record.Entity()

	// Skip entity upsert only for bare records that exist solely to carry edges
	// (no properties, no name, no description — just a URN and type for edge context).
	edgeOnly := entity.GetProperties() == nil && entity.GetName() == "" && entity.GetDescription() == ""
	if !edgeOnly {
		entityReq := s.buildEntityRequest(record)
		if err := s.post(ctx, upsertEntityRoute, entityReq); err != nil {
			return fmt.Errorf("upsert entity: %w", err)
		}
	}

	// Upsert all edges uniformly via UpsertEdge.
	for _, edge := range record.Edges() {
		edgeReq := UpsertEdgeRequest{
			SourceURN: edge.GetSourceUrn(),
			TargetURN: edge.GetTargetUrn(),
			Type:      edge.GetType(),
			Source:    edge.GetSource(),
		}
		if edge.GetProperties() != nil {
			edgeReq.Properties = edge.GetProperties().AsMap()
		}
		if edgeReq.Source == "" {
			edgeReq.Source = entity.GetSource()
		}
		if err := s.post(ctx, upsertEdgeRoute, edgeReq); err != nil {
			return fmt.Errorf("upsert %s edge for %s -> %s: %w", edge.GetType(), edge.GetSourceUrn(), edge.GetTargetUrn(), err)
		}
	}

	return nil
}

func (s *Sink) buildEntityRequest(record models.Record) UpsertEntityRequest {
	entity := record.Entity()
	req := UpsertEntityRequest{
		URN:         entity.GetUrn(),
		Type:        entity.GetType(),
		Name:        entity.GetName(),
		Description: entity.GetDescription(),
		Source:      entity.GetSource(),
	}

	if entity.GetProperties() != nil {
		req.Properties = entity.GetProperties().AsMap()
	}

	return req
}

const (
	upsertEntityRoute = "/raystack.compass.v1beta1.CompassService/UpsertEntity"
	upsertEdgeRoute   = "/raystack.compass.v1beta1.CompassService/UpsertEdge"
)

func (s *Sink) post(ctx context.Context, route string, payload any) error {
	targetURL := s.urlb.New().Path(route).URL()

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL.String(), bytes.NewBuffer(payloadBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req = otelhttpclient.AnnotateRequest(req, route)

	for hdrKey, hdrVal := range s.config.Headers {
		hdrVals := strings.Split(hdrVal, ",")
		for _, val := range hdrVals {
			req.Header.Add(hdrKey, strings.TrimSpace(val))
		}
	}

	res, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer plugins.DrainBody(res)

	if res.StatusCode == http.StatusOK {
		return nil
	}

	respBody, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}

	err = fmt.Errorf("compass returns %d: %v", res.StatusCode, string(respBody))
	switch code := res.StatusCode; {
	case code >= 500:
		return plugins.NewRetryError(err)
	default:
		return err
	}
}

func init() {
	if err := registry.Sinks.Register("compass", func() plugins.Syncer {
		return New(&http.Client{}, plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
