package dbt

// Manifest represents a dbt manifest.json file.
type Manifest struct {
	Nodes   map[string]ManifestNode   `json:"nodes"`
	Sources map[string]ManifestSource `json:"sources"`
}

// ManifestNode represents a model, seed, snapshot, or test in the manifest.
type ManifestNode struct {
	UniqueID     string                   `json:"unique_id"`
	ResourceType string                   `json:"resource_type"`
	Name         string                   `json:"name"`
	Schema       string                   `json:"schema"`
	Database     string                   `json:"database"`
	Description  string                   `json:"description"`
	Columns      map[string]ManifestColumn `json:"columns"`
	DependsOn    DependsOn                `json:"depends_on"`
	Tags         []string                 `json:"tags"`
	Meta         map[string]any           `json:"meta"`
	Config       NodeConfig               `json:"config"`
	Path         string                   `json:"path"`
	Language     string                   `json:"language"`
}

// ManifestSource represents a source definition in the manifest.
type ManifestSource struct {
	UniqueID    string                   `json:"unique_id"`
	Name        string                   `json:"name"`
	SourceName  string                   `json:"source_name"`
	Schema      string                   `json:"schema"`
	Database    string                   `json:"database"`
	Description string                   `json:"description"`
	Columns     map[string]ManifestColumn `json:"columns"`
	Loader      string                   `json:"loader"`
	Tags        []string                 `json:"tags"`
	Meta        map[string]any           `json:"meta"`
}

// ManifestColumn represents a column definition.
type ManifestColumn struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	DataType    string `json:"data_type"`
}

// DependsOn lists upstream dependencies of a node.
type DependsOn struct {
	Nodes []string `json:"nodes"`
}

// NodeConfig holds dbt node configuration.
type NodeConfig struct {
	Materialized string `json:"materialized"`
}

// Catalog represents a dbt catalog.json file.
type Catalog struct {
	Nodes map[string]CatalogNode `json:"nodes"`
}

// CatalogNode holds physical table metadata from the warehouse.
type CatalogNode struct {
	Columns map[string]CatalogColumn `json:"columns"`
	Stats   map[string]CatalogStat   `json:"stats"`
}

// CatalogColumn holds actual column metadata from the warehouse.
type CatalogColumn struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Index   int    `json:"index"`
	Comment string `json:"comment"`
}

// CatalogStat holds a table-level statistic.
type CatalogStat struct {
	ID          string `json:"id"`
	Label       string `json:"label"`
	Value       any    `json:"value"`
	Description string `json:"description"`
}
