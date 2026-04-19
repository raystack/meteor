package confluence

import (
	"context"
	_ "embed"
	"fmt"
	"regexp"
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

type Config struct {
	BaseURL  string   `json:"base_url" yaml:"base_url" mapstructure:"base_url" validate:"required,url"`
	Username string   `json:"username" yaml:"username" mapstructure:"username" validate:"required"`
	Token    string   `json:"token" yaml:"token" mapstructure:"token" validate:"required"`
	Spaces   []string `json:"spaces" yaml:"spaces" mapstructure:"spaces"`
	Exclude  []string `json:"exclude" yaml:"exclude" mapstructure:"exclude"`
}

var sampleConfig = `
# Confluence base URL (required)
base_url: https://mycompany.atlassian.net/wiki
# Atlassian account email (required)
username: user@company.com
# Atlassian API token (required)
token: your-api-token
# Filter to specific space keys (optional, defaults to all spaces)
spaces:
  - ENG
  - DATA
# Exclude space keys (optional)
exclude:
  - ARCHIVE`

var info = plugins.Info{
	Description:  "Page metadata and relationships from Confluence spaces.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"saas", "collaboration"},
	Entities: []plugins.EntityInfo{
		{Type: "document", URNPattern: "urn:confluence:{scope}:document:{page_id}"},
	},
	Edges: []plugins.EdgeInfo{
		{Type: "child_of", From: "document", To: "document"},
		{Type: "owned_by", From: "document", To: "user"},
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

	e.client = NewClient(e.config.BaseURL, e.config.Username, e.config.Token)

	e.exclude = make(map[string]bool, len(e.config.Exclude))
	for _, key := range e.config.Exclude {
		e.exclude[key] = true
	}

	return nil
}

func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	spaces, err := e.client.GetSpaces(ctx, e.config.Spaces)
	if err != nil {
		return fmt.Errorf("get spaces: %w", err)
	}

	for _, space := range spaces {
		if e.exclude[space.Key] {
			continue
		}

		emit(e.buildSpaceRecord(space))

		if err := e.extractPages(ctx, emit, space); err != nil {
			e.logger.Warn("failed to extract pages from space, skipping",
				"space", space.Key, "error", err)
		}
	}

	return nil
}

func (e *Extractor) extractPages(ctx context.Context, emit plugins.Emit, space Space) error {
	pages, err := e.client.GetPages(ctx, space.ID)
	if err != nil {
		return err
	}

	spaceURN := models.NewURN("confluence", e.UrnScope, "space", space.Key)
	for _, page := range pages {
		labels, err := e.client.GetPageLabels(ctx, page.ID)
		if err != nil {
			e.logger.Warn("failed to get labels for page, skipping labels",
				"page_id", page.ID, "error", err)
		}
		emit(e.buildPageRecord(page, space, spaceURN, labels))
	}

	return nil
}

func (e *Extractor) buildSpaceRecord(space Space) models.Record {
	urn := models.NewURN("confluence", e.UrnScope, "space", space.Key)

	props := map[string]any{
		"space_key":  space.Key,
		"space_type": space.Type,
		"status":     space.Status,
	}
	if space.Links.WebUI != "" {
		props["web_url"] = e.config.BaseURL + space.Links.WebUI
	}

	entity := models.NewEntity(urn, "space", space.Name, "confluence", props)
	if desc := space.Description.Plain.Value; desc != "" {
		entity.Description = desc
	}

	return models.NewRecord(entity)
}

func (e *Extractor) buildPageRecord(page Page, space Space, spaceURN string, labels []Label) models.Record {
	urn := models.NewURN("confluence", e.UrnScope, "document", page.ID)

	labelNames := make([]string, 0, len(labels))
	for _, l := range labels {
		labelNames = append(labelNames, l.Name)
	}

	props := map[string]any{
		"page_id":    page.ID,
		"space_key":  space.Key,
		"status":     page.Status,
		"version":    page.Version.Number,
		"created_at": page.CreatedAt.UTC().Format(time.RFC3339),
		"updated_at": page.Version.CreatedAt.UTC().Format(time.RFC3339),
	}
	if len(labelNames) > 0 {
		props["labels"] = labelNames
	}
	if page.Links.WebUI != "" {
		props["web_url"] = e.config.BaseURL + page.Links.WebUI
	}

	entity := models.NewEntity(urn, "document", page.Title, "confluence", props)

	var edges []*meteorv1beta1.Edge

	// Page belongs to space.
	edges = append(edges, &meteorv1beta1.Edge{
		SourceUrn: urn,
		TargetUrn: spaceURN,
		Type:      "belongs_to",
		Source:    "confluence",
	})

	// Page hierarchy: child_of parent page.
	if page.ParentID != "" {
		parentURN := models.NewURN("confluence", e.UrnScope, "document", page.ParentID)
		edges = append(edges, &meteorv1beta1.Edge{
			SourceUrn: urn,
			TargetUrn: parentURN,
			Type:      "child_of",
			Source:    "confluence",
		})
	}

	// Owner: page author.
	if page.AuthorID != "" {
		ownerURN := models.NewURN("confluence", e.UrnScope, "user", page.AuthorID)
		edges = append(edges, models.OwnerEdge(urn, ownerURN, "confluence"))
	}

	// Scan page body for URN references to link to data assets.
	if body := page.Body.Storage.Value; body != "" {
		for _, ref := range extractURNReferences(body) {
			edges = append(edges, &meteorv1beta1.Edge{
				SourceUrn: urn,
				TargetUrn: ref,
				Type:      "documented_by",
				Source:    "confluence",
			})
		}
	}

	return models.NewRecord(entity, edges...)
}

// urnPattern matches URN references embedded in page content.
var urnPattern = regexp.MustCompile(`urn:[a-zA-Z0-9_-]+:[a-zA-Z0-9_.-]+:[a-zA-Z0-9_-]+:[a-zA-Z0-9_./-]+`)

// extractURNReferences finds URN strings in HTML/storage-format content.
func extractURNReferences(body string) []string {
	matches := urnPattern.FindAllString(body, -1)
	seen := make(map[string]bool, len(matches))
	var unique []string
	for _, m := range matches {
		cleaned := strings.TrimRight(m, ".,;:!?\"')")
		if !seen[cleaned] {
			seen[cleaned] = true
			unique = append(unique, cleaned)
		}
	}
	return unique
}

func init() {
	if err := registry.Extractors.Register("confluence", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
