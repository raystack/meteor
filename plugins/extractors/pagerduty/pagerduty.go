package pagerduty

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
)

//go:embed README.md
var summary string

// Config holds the extractor configuration.
type Config struct {
	APIKey       string   `json:"api_key" yaml:"api_key" mapstructure:"api_key" validate:"required"`
	Exclude      []string `json:"exclude" yaml:"exclude" mapstructure:"exclude"`
	IncidentDays int      `json:"incident_days" yaml:"incident_days" mapstructure:"incident_days"`
}

var sampleConfig = `
# PagerDuty API key (required)
api_key: your-pagerduty-api-key
# Service IDs to exclude (optional)
exclude:
  - PABC123
# Number of days to look back for incidents (optional, default 30)
incident_days: 30`

var info = plugins.Info{
	Description:  "Service and incident metadata from PagerDuty.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"saas", "incident"},
	Entities: []plugins.EntityInfo{
		{Type: "service", URNPattern: "urn:pagerduty:{scope}:service:{service_id}"},
		{Type: "incident", URNPattern: "urn:pagerduty:{scope}:incident:{incident_id}"},
	},
	Edges: []plugins.EdgeInfo{
		{Type: "owned_by", From: "service", To: "team"},
		{Type: "belongs_to", From: "incident", To: "service"},
	},
}

// Extractor extracts metadata from PagerDuty.
type Extractor struct {
	plugins.BaseExtractor
	logger  log.Logger
	config  Config
	client  *Client
	exclude map[string]bool
}

// New creates a new PagerDuty extractor.
func New(logger log.Logger) *Extractor {
	e := &Extractor{logger: logger}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)
	return e
}

// Init initialises the extractor with the given config.
func (e *Extractor) Init(ctx context.Context, config plugins.Config) error {
	if err := e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	e.client = NewClient(e.config.APIKey)

	e.exclude = make(map[string]bool, len(e.config.Exclude))
	for _, id := range e.config.Exclude {
		e.exclude[id] = true
	}

	return nil
}

// Extract extracts services and incidents from PagerDuty.
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	services, err := e.client.ListServices(ctx)
	if err != nil {
		return fmt.Errorf("list services: %w", err)
	}

	serviceIDs := make(map[string]bool, len(services))
	for _, svc := range services {
		if e.exclude[svc.ID] {
			continue
		}
		serviceIDs[svc.ID] = true
		emit(e.buildServiceRecord(svc))
	}

	days := e.config.IncidentDays
	if days <= 0 {
		days = 30
	}
	now := time.Now().UTC()
	since := now.AddDate(0, 0, -days)

	incidents, err := e.client.ListIncidents(ctx, since, now)
	if err != nil {
		return fmt.Errorf("list incidents: %w", err)
	}

	for _, inc := range incidents {
		if e.exclude[inc.Service.ID] || !serviceIDs[inc.Service.ID] {
			continue
		}
		emit(e.buildIncidentRecord(inc))
	}

	return nil
}

func (e *Extractor) buildServiceRecord(svc Service) models.Record {
	urn := models.NewURN("pagerduty", e.UrnScope, "service", svc.ID)

	teamIDs := make([]string, 0, len(svc.Teams))
	for _, t := range svc.Teams {
		teamIDs = append(teamIDs, t.ID)
	}

	props := map[string]any{
		"status":                svc.Status,
		"description":          svc.Description,
		"created_at":           svc.CreatedAt,
		"updated_at":           svc.UpdatedAt,
		"html_url":             svc.HTMLURL,
		"escalation_policy_id": svc.EscalationPolicy.ID,
		"alert_creation":       svc.AlertCreation,
		"incident_urgency_rule": svc.IncidentUrgencyRule.Type,
	}
	if len(teamIDs) > 0 {
		props["team_ids"] = strings.Join(teamIDs, ",")
	}

	entity := models.NewEntity(urn, "service", svc.Name, "pagerduty", props)
	if svc.Description != "" {
		entity.Description = svc.Description
	}

	var edges []*meteorv1beta1.Edge
	for _, team := range svc.Teams {
		ownerURN := models.NewURN("pagerduty", e.UrnScope, "team", team.ID)
		edges = append(edges, models.OwnerEdge(urn, ownerURN, "pagerduty"))
	}

	return models.NewRecord(entity, edges...)
}

func (e *Extractor) buildIncidentRecord(inc Incident) models.Record {
	urn := models.NewURN("pagerduty", e.UrnScope, "incident", inc.ID)

	props := map[string]any{
		"status":          inc.Status,
		"urgency":         inc.Urgency,
		"created_at":      inc.CreatedAt,
		"html_url":        inc.HTMLURL,
		"incident_number": inc.IncidentNumber,
		"title":           inc.Title,
	}
	if inc.ResolvedAt != "" {
		props["resolved_at"] = inc.ResolvedAt
	}
	if inc.Priority != nil {
		props["priority"] = inc.Priority.Summary
	}

	entity := models.NewEntity(urn, "incident", inc.Title, "pagerduty", props)

	var edges []*meteorv1beta1.Edge
	if inc.Service.ID != "" {
		serviceURN := models.NewURN("pagerduty", e.UrnScope, "service", inc.Service.ID)
		edges = append(edges, &meteorv1beta1.Edge{
			SourceUrn: urn,
			TargetUrn: serviceURN,
			Type:      "belongs_to",
			Source:    "pagerduty",
		})
	}

	return models.NewRecord(entity, edges...)
}

func init() {
	if err := registry.Extractors.Register("pagerduty", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
