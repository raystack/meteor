package opsgenie

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
)

//go:embed README.md
var summary string

const defaultBaseURL = "https://api.opsgenie.com"

type Config struct {
	APIKey  string   `json:"api_key" yaml:"api_key" mapstructure:"api_key" validate:"required"`
	BaseURL string   `json:"base_url" yaml:"base_url" mapstructure:"base_url" validate:"omitempty,url"`
	Exclude []string `json:"exclude" yaml:"exclude" mapstructure:"exclude"`
}

var sampleConfig = `
# OpsGenie API key (required)
api_key: your-api-key
# OpsGenie API base URL (optional, defaults to https://api.opsgenie.com)
# Use https://api.eu.opsgenie.com for EU instances
base_url: https://api.opsgenie.com
# Service IDs to exclude (optional)
exclude:
  - service-id-to-skip`

var info = plugins.Info{
	Description:  "Service and incident metadata from OpsGenie.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"saas", "incident"},
	Entities: []plugins.EntityInfo{
		{Type: "service", URNPattern: "urn:opsgenie:{scope}:service:{service_id}"},
		{Type: "incident", URNPattern: "urn:opsgenie:{scope}:incident:{incident_id}"},
	},
	Edges: []plugins.EdgeInfo{
		{Type: "owned_by", From: "service", To: "team"},
		{Type: "belongs_to", From: "incident", To: "service"},
	},
}

type Extractor struct {
	plugins.BaseExtractor
	logger  log.Logger
	config  Config
	client  *Client
	exclude map[string]bool
}

func New(logger log.Logger) *Extractor {
	e := &Extractor{logger: logger}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)
	return e
}

func (e *Extractor) Init(ctx context.Context, config plugins.Config) error {
	if err := e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	baseURL := e.config.BaseURL
	if baseURL == "" {
		baseURL = defaultBaseURL
	}

	e.client = NewClient(baseURL, e.config.APIKey)

	e.exclude = make(map[string]bool, len(e.config.Exclude))
	for _, id := range e.config.Exclude {
		e.exclude[id] = true
	}

	return nil
}

func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	if err := e.extractServices(ctx, emit); err != nil {
		return fmt.Errorf("extract services: %w", err)
	}

	if err := e.extractIncidents(ctx, emit); err != nil {
		return fmt.Errorf("extract incidents: %w", err)
	}

	return nil
}

func (e *Extractor) extractServices(ctx context.Context, emit plugins.Emit) error {
	services, err := e.client.ListServices(ctx)
	if err != nil {
		return err
	}

	for _, svc := range services {
		if e.exclude[svc.ID] {
			continue
		}
		emit(e.buildServiceRecord(svc))
	}

	return nil
}

func (e *Extractor) extractIncidents(ctx context.Context, emit plugins.Emit) error {
	incidents, err := e.client.ListIncidents(ctx)
	if err != nil {
		return err
	}

	for _, inc := range incidents {
		emit(e.buildIncidentRecord(inc))
	}

	return nil
}

func (e *Extractor) buildServiceRecord(svc Service) models.Record {
	urn := models.NewURN("opsgenie", e.UrnScope, "service", svc.ID)

	props := map[string]any{
		"description": svc.Description,
		"team_id":     svc.TeamID,
		"html_url":    fmt.Sprintf("https://app.opsgenie.com/service/%s", svc.ID),
	}

	entity := models.NewEntity(urn, "service", svc.Name, "opsgenie", props)
	if svc.Description != "" {
		entity.Description = svc.Description
	}

	var edges []*meteorv1beta1.Edge

	if svc.TeamID != "" {
		teamURN := models.NewURN("opsgenie", e.UrnScope, "team", svc.TeamID)
		edges = append(edges, models.OwnerEdge(urn, teamURN, "opsgenie"))
	}

	return models.NewRecord(entity, edges...)
}

func (e *Extractor) buildIncidentRecord(inc Incident) models.Record {
	urn := models.NewURN("opsgenie", e.UrnScope, "incident", inc.ID)

	props := map[string]any{
		"status":      inc.Status,
		"priority":    inc.Priority,
		"created_at":  inc.CreatedAt,
		"resolved_at": inc.ResolvedAt,
		"html_url":    fmt.Sprintf("https://app.opsgenie.com/incident/detail/%s", inc.ID),
		"message":     inc.Message,
		"owner_id":    inc.OwnerTeam,
	}
	if len(inc.Tags) > 0 {
		props["tags"] = inc.Tags
	}

	entity := models.NewEntity(urn, "incident", inc.Message, "opsgenie", props)

	var edges []*meteorv1beta1.Edge

	for _, svcID := range inc.ImpactedServices {
		svcURN := models.NewURN("opsgenie", e.UrnScope, "service", svcID)
		edges = append(edges, &meteorv1beta1.Edge{
			SourceUrn: urn,
			TargetUrn: svcURN,
			Type:      "belongs_to",
			Source:    "opsgenie",
		})
	}

	return models.NewRecord(entity, edges...)
}

func init() {
	if err := registry.Extractors.Register("opsgenie", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
