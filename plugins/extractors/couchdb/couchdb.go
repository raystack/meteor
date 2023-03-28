package couchdb

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"
	"reflect"
	"strings"

	_ "github.com/go-kivik/couchdb"
	"github.com/go-kivik/kivik"
	"github.com/odpf/meteor/models"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/sqlutil"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
	"google.golang.org/protobuf/types/known/anypb"
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
	ConnectionURL string `json:"connection_url" yaml:"connection_url" mapstructure:"connection_url" validate:"required"`
	Exclude       string `json:"exclude" yaml:"exclude" mapstructure:"exclude"`
}

var sampleConfig = `
connection_url: http://admin:pass123@localhost:3306/
exclude: database_a,database_b`

var info = plugins.Info{
	Description:  "Table metadata from CouchDB server,",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "extractor"},
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
	excludedList := append(defaultDBList, strings.Split(e.config.Exclude, ",")...)
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
	var columns []*v1beta2.Column
	columns, err = e.extractColumns(ctx, docID)
	if err != nil {
		return
	}
	table, err := anypb.New(&v1beta2.Table{
		Columns: columns,
	})
	if err != nil {
		err = fmt.Errorf("error creating Any struct for test: %w", err)
		return err
	}
	// push table to channel
	e.emit(models.NewRecord(&v1beta2.Asset{
		Urn:     models.NewURN("couchdb", e.UrnScope, "table", fmt.Sprintf("%s.%s", dbName, docID)),
		Name:    docID,
		Type:    "table",
		Service: "couchdb",
		Data:    table,
	}))

	return
}

// Extract columns from a given table
func (e *Extractor) extractColumns(ctx context.Context, docID string) (columns []*v1beta2.Column, err error) {
	size, rev, err := e.db.GetMeta(ctx, docID)
	if err != nil {
		return
	}
	row := e.db.Get(ctx, docID)
	var fields map[string]interface{}
	err = row.ScanDoc(&fields)
	if err != nil {
		return
	}

	for k := range fields {
		if k == "_id" || k == "_rev" {
			continue
		}

		columns = append(columns, &v1beta2.Column{
			Name:        k,
			DataType:    reflect.ValueOf(fields[k]).Kind().String(),
			Description: rev,
			Length:      size,
		})
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
