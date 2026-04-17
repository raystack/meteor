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
	Host    string            `json:"host" yaml:"host" mapstructure:"host" validate:"required"`
	Headers map[string]string `json:"headers" yaml:"headers" mapstructure:"headers"`
}

var info = plugins.Info{
	Description: "Send metadata to compass http service",
	Summary:     summary,
	Tags:        []string{"http", "sink"},
	SampleConfig: heredoc.Doc(`
	# The hostname of the compass service
	host: https://compass.com
	# Additional HTTP headers send to compass, multiple headers value are separated by a comma
	headers:
	  Compass-User-UUID: meteor@raystack.io
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
	errGroup.SetLimit(len(batch))

	for _, record := range batch {
		record := record
		errGroup.Go(func() error {
			entity := record.Entity()
			s.logger.Info("sinking record to compass", "record", entity.GetUrn())

			if err := s.sinkRecord(ctx, record); err != nil {
				return fmt.Errorf("sink asset %s: %w", entity.GetUrn(), err)
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

	// 1. Upsert the entity.
	entityReq := s.buildEntityRequest(record)
	if err := s.post(ctx, upsertEntityRoute, entityReq); err != nil {
		return fmt.Errorf("upsert entity: %w", err)
	}

	// 2. Upsert non-lineage edges (lineage is sent inline with the entity).
	for _, edge := range record.Edges() {
		if edge.GetType() == "lineage" {
			continue
		}
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

	// Copy properties from entity
	if entity.GetProperties() != nil {
		req.Properties = entity.GetProperties().AsMap()
	}

	// Extract upstreams/downstreams from lineage edges
	for _, edge := range record.Edges() {
		if edge.GetType() != "lineage" {
			continue
		}
		if edge.GetSourceUrn() == entity.GetUrn() {
			req.Downstreams = append(req.Downstreams, edge.GetTargetUrn())
		} else if edge.GetTargetUrn() == entity.GetUrn() {
			req.Upstreams = append(req.Upstreams, edge.GetSourceUrn())
		}
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
