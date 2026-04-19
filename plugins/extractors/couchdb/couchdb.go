package couchdb

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"
	"reflect"

	_ "github.com/go-kivik/couchdb"
	"github.com/go-kivik/kivik"
	"github.com/raystack/meteor/models"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/sqlutil"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
)

//go:embed README.md
var summary string

var defaultDBList = []string{
	"_global_changes",
	"_replicator",
	"_users",
}

// Config holds the connection URL for the extractor
type Config struct {
	ConnectionURL string  `json:"connection_url" yaml:"connection_url" mapstructure:"connection_url" validate:"required"`
	Exclude       Exclude `json:"exclude" yaml:"exclude" mapstructure:"exclude"`
}

// Exclude contains the list of databases to skip during extraction.
type Exclude struct {
	Databases []string `json:"databases" yaml:"databases" mapstructure:"databases"`
}

var sampleConfig = `
connection_url: http://admin:pass123@localhost:3306/
exclude:
  databases:
    - database_a
    - database_b`

var info = plugins.Info{
	Description:  "Document metadata from Apache CouchDB server.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "database"},
	Entities: []plugins.EntityInfo{
		{Type: "table", URNPattern: "urn:couchdb:{scope}:table:{database}.{doc_id}"},
	},
}

// Extractor manages the extraction of data from CouchDB
type Extractor struct {
	plugins.BaseExtractor
	client      *kivik.Client
	db          *kivik.DB
	excludedDbs map[string]bool
	logger      log.Logger
	config      Config
	emit        plugins.Emit
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger) *Extractor {
	e := &Extractor{
		logger: logger,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	return e
}

// Initialise the Extractor with Configurations
func (e *Extractor) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	// build excluded database list
	excludedList := append(defaultDBList, e.config.Exclude.Databases...)
	e.excludedDbs = sqlutil.BuildBoolMap(excludedList)

	// create client
	e.client, err = kivik.New("couch", e.config.ConnectionURL)
	if err != nil {
		return
	}

	return
}

// Extract extracts the data from the CouchDB server
// and collected through the out channel
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	defer e.client.Close(ctx)
	e.emit = emit

	res, err := e.client.AllDBs(ctx)
	if err != nil {
		return
	}

	for _, dbName := range res {
		if e.isExcludedDB(dbName) {
			continue
		}
		if err := e.extractTables(ctx, dbName); err != nil {
			return err
		}
	}
	return
}

// Extract tables from a given database
func (e *Extractor) extractTables(ctx context.Context, dbName string) (err error) {
	e.db = e.client.DB(ctx, dbName)

	// extract documents
	rows, err := e.db.AllDocs(ctx)
	if err != nil {
		return
	}

	// process each rows
	for rows.Next() {
		docID := rows.ID()
		if err := e.processTable(ctx, dbName, docID); err != nil {
			return err
		}
	}

	return
}

// Build and push document to output channel
func (e *Extractor) processTable(ctx context.Context, dbName string, docID string) (err error) {
	columns, err := e.extractColumns(ctx, docID)
	if err != nil {
		return
	}

	// push table to channel
	e.emit(models.NewRecord(models.NewEntity(
		models.NewURN("couchdb", e.UrnScope, "table", fmt.Sprintf("%s.%s", dbName, docID)),
		"table", docID, "couchdb",
		map[string]any{"columns": columns},
	)))

	return
}

// Extract columns from a given table
func (e *Extractor) extractColumns(ctx context.Context, docID string) (columns []any, err error) {
	size, rev, err := e.db.GetMeta(ctx, docID)
	if err != nil {
		return
	}
	row := e.db.Get(ctx, docID)
	var fields map[string]any
	err = row.ScanDoc(&fields)
	if err != nil {
		return
	}

	for k := range fields {
		if k == "_id" || k == "_rev" {
			continue
		}

		col := map[string]any{
			"name":      k,
			"data_type": reflect.ValueOf(fields[k]).Kind().String(),
		}
		if rev != "" {
			col["description"] = rev
		}
		if size != 0 {
			col["length"] = size
		}
		columns = append(columns, col)
	}
	return
}

func (e *Extractor) isExcludedDB(database string) bool {
	_, ok := e.excludedDbs[database]
	return ok
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("couchdb", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
