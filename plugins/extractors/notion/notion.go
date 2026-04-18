package notion

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
	Token   string   `json:"token" yaml:"token" mapstructure:"token" validate:"required"`
	BaseURL string   `json:"base_url" yaml:"base_url" mapstructure:"base_url"`
	Extract []string `json:"extract" yaml:"extract" mapstructure:"extract"`
}

var sampleConfig = `
# Notion integration token (required)
token: ntn_your_integration_token
# Entity types to extract (optional, defaults to all: ["pages", "databases"])
extract:
  - pages
  - databases`

var info = plugins.Info{
	Description:  "Extract page and database metadata from a Notion workspace.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"notion", "extractor"},
}

type Extractor struct {
	plugins.BaseExtractor
	logger  log.Logger
	config  Config
	client  *Client
	extract map[string]bool
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

	e.client = NewClient(e.config.Token)
	if e.config.BaseURL != "" {
		e.client.SetBaseURL(e.config.BaseURL)
	}

	e.extract = map[string]bool{
		"pages":     true,
		"databases": true,
	}
	if len(e.config.Extract) > 0 {
		e.extract = make(map[string]bool, len(e.config.Extract))
		for _, v := range e.config.Extract {
			e.extract[v] = true
		}
	}

	return nil
}

func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	if e.extract["pages"] {
		if err := e.extractPages(ctx, emit); err != nil {
			return fmt.Errorf("extract pages: %w", err)
		}
	}
	if e.extract["databases"] {
		if err := e.extractDatabases(ctx, emit); err != nil {
			return fmt.Errorf("extract databases: %w", err)
		}
	}
	return nil
}

func (e *Extractor) extractPages(ctx context.Context, emit plugins.Emit) error {
	pages, err := e.client.SearchPages(ctx)
	if err != nil {
		return err
	}

	for _, page := range pages {
		if page.Archived {
			continue
		}

		var bodyText string
		blocks, err := e.client.GetBlockChildren(ctx, page.ID)
		if err != nil {
			e.logger.Warn("failed to get page content, skipping URN scan",
				"page_id", page.ID, "error", err)
		} else {
			bodyText = blocksToText(blocks)
		}

		emit(e.buildPageRecord(page, bodyText))
	}

	return nil
}

func (e *Extractor) extractDatabases(ctx context.Context, emit plugins.Emit) error {
	databases, err := e.client.SearchDatabases(ctx)
	if err != nil {
		return err
	}

	for _, db := range databases {
		if db.Archived {
			continue
		}
		emit(e.buildDatabaseRecord(db))
	}

	return nil
}

func (e *Extractor) buildPageRecord(page Page, bodyText string) models.Record {
	urn := models.NewURN("notion", e.UrnScope, "document", page.ID)

	title := extractPageTitle(page.Properties)
	props := map[string]any{
		"page_id":    page.ID,
		"created_at": page.CreatedTime.UTC().Format(time.RFC3339),
		"updated_at": page.LastEditedTime.UTC().Format(time.RFC3339),
		"archived":   page.Archived,
	}
	if page.URL != "" {
		props["web_url"] = page.URL
	}
	if page.CreatedBy.Name != "" {
		props["created_by"] = page.CreatedBy.Name
	}
	if page.LastEditedBy.Name != "" {
		props["last_edited_by"] = page.LastEditedBy.Name
	}

	entity := models.NewEntity(urn, "document", title, "notion", props)

	var edges []*meteorv1beta1.Edge

	// Parent relationship.
	if parentURN, parentType := resolveParent(page.Parent, e.UrnScope); parentURN != "" {
		edges = append(edges, &meteorv1beta1.Edge{
			SourceUrn: urn,
			TargetUrn: parentURN,
			Type:      parentType,
			Source:    "notion",
		})
	}

	// Owner: page creator.
	if page.CreatedBy.ID != "" {
		ownerURN := models.NewURN("notion", e.UrnScope, "user", page.CreatedBy.ID)
		edges = append(edges, models.OwnerEdge(urn, ownerURN, "notion"))
	}

	// Scan content for URN references.
	if bodyText != "" {
		for _, ref := range extractURNReferences(bodyText) {
			edges = append(edges, &meteorv1beta1.Edge{
				SourceUrn: urn,
				TargetUrn: ref,
				Type:      "documented_by",
				Source:    "notion",
			})
		}
	}

	return models.NewRecord(entity, edges...)
}

func (e *Extractor) buildDatabaseRecord(db Database) models.Record {
	urn := models.NewURN("notion", e.UrnScope, "document", db.ID)

	title := richTextToPlain(db.Title)
	description := richTextToPlain(db.Description)

	// Extract property schema names (column names).
	var columns []string
	for name := range db.Properties {
		columns = append(columns, name)
	}

	props := map[string]any{
		"database_id": db.ID,
		"created_at":  db.CreatedTime.UTC().Format(time.RFC3339),
		"updated_at":  db.LastEditedTime.UTC().Format(time.RFC3339),
		"archived":    db.Archived,
	}
	if db.URL != "" {
		props["web_url"] = db.URL
	}
	if db.CreatedBy.Name != "" {
		props["created_by"] = db.CreatedBy.Name
	}
	if len(columns) > 0 {
		props["columns"] = columns
	}

	entity := models.NewEntity(urn, "document", title, "notion", props)
	if description != "" {
		entity.Description = description
	}

	var edges []*meteorv1beta1.Edge

	// Parent relationship.
	if parentURN, parentType := resolveParent(db.Parent, e.UrnScope); parentURN != "" {
		edges = append(edges, &meteorv1beta1.Edge{
			SourceUrn: urn,
			TargetUrn: parentURN,
			Type:      parentType,
			Source:    "notion",
		})
	}

	// Owner: database creator.
	if db.CreatedBy.ID != "" {
		ownerURN := models.NewURN("notion", e.UrnScope, "user", db.CreatedBy.ID)
		edges = append(edges, models.OwnerEdge(urn, ownerURN, "notion"))
	}

	return models.NewRecord(entity, edges...)
}

// resolveParent returns (parentURN, edgeType) for the given parent.
func resolveParent(parent Parent, scope string) (string, string) {
	switch parent.Type {
	case "page_id":
		return models.NewURN("notion", scope, "document", parent.PageID), "child_of"
	case "database_id":
		return models.NewURN("notion", scope, "document", parent.DatabaseID), "belongs_to"
	default:
		return "", ""
	}
}

// extractPageTitle extracts the title from page properties.
// Notion pages have a "title" type property (often named "Name" or "title").
func extractPageTitle(properties map[string]any) string {
	for _, v := range properties {
		prop, ok := v.(map[string]any)
		if !ok {
			continue
		}
		if prop["type"] != "title" {
			continue
		}
		titleArr, ok := prop["title"].([]any)
		if !ok || len(titleArr) == 0 {
			continue
		}
		for _, item := range titleArr {
			rt, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if text, ok := rt["plain_text"].(string); ok {
				return text
			}
		}
	}
	return "Untitled"
}

// richTextToPlain concatenates plain text from a rich text array.
func richTextToPlain(texts []RichText) string {
	var parts []string
	for _, t := range texts {
		if t.PlainText != "" {
			parts = append(parts, t.PlainText)
		}
	}
	return strings.Join(parts, "")
}

// blocksToText concatenates plain text from all blocks.
func blocksToText(blocks []Block) string {
	var parts []string
	for _, b := range blocks {
		if text := b.PlainText(); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "\n")
}

// urnPattern matches URN references embedded in page content.
var urnPattern = regexp.MustCompile(`urn:[a-zA-Z0-9_-]+:[a-zA-Z0-9_.-]+:[a-zA-Z0-9_-]+:[a-zA-Z0-9_./-]+`)

// extractURNReferences finds URN strings in text content.
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
	if err := registry.Extractors.Register("notion", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
