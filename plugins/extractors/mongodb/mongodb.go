package mongodb

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"
	"sort"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/odpf/meteor/models"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//go:embed README.md
var summary string

var defaultCollections = []string{
	"system.users",
	"system.version",
	"system.sessions",
	"startup_log",
}

// Config holds the connection URL for the extractor
type Config struct {
	ConnectionURL string  `json:"connection_url" yaml:"connection_url" mapstructure:"connection_url" validate:"required"`
	Exclude       Exclude `json:"exclude" yaml:"exclude" mapstructure:"exclude"`
}

type Exclude struct {
	Databases   []string `json:"databases" yaml:"databases" mapstructure:"databases"`
	Collections []string `json:"collections" yaml:"collections" mapstructure:"collections"`
}

var sampleConfig = `
connection_url: "mongodb://admin:pass123@localhost:3306"
exclude:
  databases:
	- database_a
	- database_b
  collections:
	- dataset_c.table_a`

var info = plugins.Info{
	Description:  "Collection metadata from MongoDB Server",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "extractor"},
}

// Extractor manages the communication with the mongo server
type Extractor struct {
	plugins.BaseExtractor
	// internal states
	client *mongo.Client
	logger log.Logger
	config Config
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger) *Extractor {
	e := &Extractor{
		logger: logger,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	return e
}

func (e *Extractor) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	// setup client
	if e.client, err = createAndConnnectClient(ctx, e.config.ConnectionURL); err != nil {
		return errors.Wrap(err, "failed to create client")
	}

	return
}

// Extract collects metadata of each database through emitter
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	databases, err := e.client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return errors.Wrap(err, "failed to list database names")
	}

	for _, dbName := range databases {
		if e.isExcludedDB(dbName, e.config.Exclude.Databases) {
			continue
		}

		database := e.client.Database(dbName)
		if err := e.extractCollections(ctx, database, emit); err != nil {
			return errors.Wrap(err, "failed to extract collections")
		}
	}

	return
}

// Extract and output collections from a single mongo database
func (e *Extractor) extractCollections(ctx context.Context, db *mongo.Database, emit plugins.Emit) (err error) {
	collections, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return errors.Wrap(err, "failed to list collection names")
	}

	// we need to sort the collections for testing purpose
	// this ensures the returned collection list are in consistent order
	// or else test might fail
	sort.Strings(collections)
	for _, collectionName := range collections {
		// skip if collection is default mongo or is in the user's exclude collection list
		if e.isExcludedCollection(collectionName, db.Name(), e.config.Exclude.Collections) {
			continue
		}

		table, err := e.buildTable(ctx, db, collectionName)
		if err != nil {
			return errors.Wrap(err, "failed to build table")
		}

		emit(models.NewRecord(table))
	}

	return
}

// Build table metadata model from a collection
func (e *Extractor) buildTable(ctx context.Context, db *mongo.Database, collectionName string) (table *v1beta2.Asset, err error) {
	// get total rows
	totalRows, err := db.Collection(collectionName).EstimatedDocumentCount(ctx)
	if err != nil {
		err = errors.Wrap(err, "failed to fetch total no of rows")
		return
	}
	data, err := anypb.New(&v1beta2.Table{
		Profile: &v1beta2.TableProfile{
			TotalRows: totalRows,
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to build Any struct")
	}
	//
	table = &v1beta2.Asset{
		Urn:     models.NewURN("mongodb", e.UrnScope, "collection", fmt.Sprintf("%s.%s", db.Name(), collectionName)),
		Name:    collectionName,
		Service: "mongodb",
		Type:    "table",
		Data:    data,
	}

	return
}

// Check if collection is default or in user's exclude list
func (e *Extractor) isExcludedCollection(collName string, dbName string, excludedCollections []string) bool {
	collectionName := fmt.Sprintf("%s.%s", dbName, collName)
	excludedCollections = append(excludedCollections, defaultCollections...)

	for _, c := range excludedCollections {
		if c == collectionName {
			return true
		}
	}

	return false
}

// isExcludedDB checks if the given db is in the list of excluded databases
func (e *Extractor) isExcludedDB(dbName string, excludeDatabases []string) bool {
	for _, d := range excludeDatabases {
		if d == dbName {
			return true
		}
	}

	return false
}

// Create mongo client and tries to connect
func createAndConnnectClient(ctx context.Context, uri string) (client *mongo.Client, err error) {
	clientOptions := options.Client().ApplyURI(uri)
	client, err = mongo.NewClient(clientOptions)
	if err != nil {
		return
	}
	err = client.Connect(ctx)
	if err != nil {
		return
	}

	return
}

func init() {
	if err := registry.Extractors.Register("mongodb", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
