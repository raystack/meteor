package dbt

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
)

//go:embed README.md
var summary string

type Config struct {
	Manifest string `json:"manifest" yaml:"manifest" mapstructure:"manifest" validate:"required,file"`
	Catalog  string `json:"catalog" yaml:"catalog" mapstructure:"catalog" validate:"omitempty,file"`
}

var sampleConfig = `
# Path to dbt manifest.json (required)
manifest: target/manifest.json
# Path to dbt catalog.json for column types and stats (optional)
# catalog: target/catalog.json`

var info = plugins.Info{
	Description:  "Model and source metadata from dbt manifest.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "orchestration"},
	Entities: []plugins.EntityInfo{
		{Type: "model", URNPattern: "urn:dbt:{scope}:model:{model_path}"},
		{Type: "source", URNPattern: "urn:dbt:{scope}:source:{source_path}"},
	},
	Edges: []plugins.EdgeInfo{
		{Type: "derived_from", From: "model", To: "model"},
		{Type: "owned_by", From: "model", To: "user"},
	},
}

type Extractor struct {
	plugins.BaseExtractor
	logger   log.Logger
	config   Config
	manifest Manifest
	catalog  *Catalog
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

	manifest, err := readJSON[Manifest](e.config.Manifest)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	e.manifest = manifest

	if e.config.Catalog != "" {
		catalog, err := readJSON[Catalog](e.config.Catalog)
		if err != nil {
			return fmt.Errorf("read catalog: %w", err)
		}
		e.catalog = &catalog
	}

	return nil
}

func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	for _, node := range e.manifest.Nodes {
		if node.ResourceType != "model" {
			continue
		}
		emit(e.buildModelRecord(node))
	}

	for _, src := range e.manifest.Sources {
		emit(e.buildSourceRecord(src))
	}

	return nil
}

func (e *Extractor) buildModelRecord(node ManifestNode) models.Record {
	urn := models.NewURN("dbt", e.UrnScope, "model", node.UniqueID)

	columns := e.buildColumns(node.UniqueID, node.Columns)
	props := map[string]any{
		"database":        node.Database,
		"schema":          node.Schema,
		"materialization": node.Config.Materialized,
		"sql_path":        node.Path,
	}
	if node.Language != "" {
		props["language"] = node.Language
	}
	if len(node.Tags) > 0 {
		props["tags"] = node.Tags
	}
	if len(node.Meta) > 0 {
		props["meta"] = node.Meta
	}
	if len(columns) > 0 {
		props["columns"] = columns
	}

	entity := models.NewEntity(urn, "model", node.Name, "dbt", props)
	if node.Description != "" {
		entity.Description = node.Description
	}

	var edges []*meteorv1beta1.Edge
	for _, dep := range node.DependsOn.Nodes {
		depURN := e.resolveDepURN(dep)
		edges = append(edges, models.DerivedFromEdge(urn, depURN, "dbt"))
	}
	if owner := getOwner(node.Meta); owner != "" {
		ownerURN := models.NewURN("dbt", e.UrnScope, "user", owner)
		edges = append(edges, models.OwnerEdge(urn, ownerURN, "dbt"))
	}

	return models.NewRecord(entity, edges...)
}

func (e *Extractor) buildSourceRecord(src ManifestSource) models.Record {
	urn := models.NewURN("dbt", e.UrnScope, "source", src.UniqueID)

	columns := e.buildSourceColumns(src.Columns)
	props := map[string]any{
		"database":    src.Database,
		"schema":      src.Schema,
		"source_name": src.SourceName,
	}
	if src.Loader != "" {
		props["loader"] = src.Loader
	}
	if len(src.Tags) > 0 {
		props["tags"] = src.Tags
	}
	if len(src.Meta) > 0 {
		props["meta"] = src.Meta
	}
	if len(columns) > 0 {
		props["columns"] = columns
	}

	entity := models.NewEntity(urn, "source", src.Name, "dbt", props)
	if src.Description != "" {
		entity.Description = src.Description
	}

	var edges []*meteorv1beta1.Edge
	if owner := getOwner(src.Meta); owner != "" {
		ownerURN := models.NewURN("dbt", e.UrnScope, "user", owner)
		edges = append(edges, models.OwnerEdge(urn, ownerURN, "dbt"))
	}

	return models.NewRecord(entity, edges...)
}

func (e *Extractor) buildColumns(nodeID string, manifestCols map[string]ManifestColumn) []map[string]any {
	var columns []map[string]any
	for _, col := range manifestCols {
		c := map[string]any{
			"name": col.Name,
		}
		if col.Description != "" {
			c["description"] = col.Description
		}
		if col.DataType != "" {
			c["data_type"] = col.DataType
		}

		// Enrich with catalog data if available.
		if e.catalog != nil {
			if catNode, ok := e.catalog.Nodes[nodeID]; ok {
				if catCol, ok := catNode.Columns[strings.ToLower(col.Name)]; ok {
					if catCol.Type != "" {
						c["data_type"] = catCol.Type
					}
				}
			}
		}

		columns = append(columns, c)
	}
	return columns
}

func (e *Extractor) buildSourceColumns(manifestCols map[string]ManifestColumn) []map[string]any {
	var columns []map[string]any
	for _, col := range manifestCols {
		c := map[string]any{
			"name": col.Name,
		}
		if col.Description != "" {
			c["description"] = col.Description
		}
		if col.DataType != "" {
			c["data_type"] = col.DataType
		}
		columns = append(columns, c)
	}
	return columns
}

// resolveDepURN maps a manifest dependency ID to a URN.
// Dependency IDs look like "model.project.name" or "source.project.src.table".
func (e *Extractor) resolveDepURN(dep string) string {
	parts := strings.SplitN(dep, ".", 2)
	kind := parts[0] // "model", "source", "seed", etc.
	return models.NewURN("dbt", e.UrnScope, kind, dep)
}

func getOwner(meta map[string]any) string {
	if meta == nil {
		return ""
	}
	if owner, ok := meta["owner"].(string); ok {
		return owner
	}
	return ""
}

func readJSON[T any](path string) (T, error) {
	var v T
	data, err := os.ReadFile(path)
	if err != nil {
		return v, err
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return v, err
	}
	return v, nil
}

func init() {
	if err := registry.Extractors.Register("dbt", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
