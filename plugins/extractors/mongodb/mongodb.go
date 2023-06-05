package mongodb

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"
	"sort"

	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/salt/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
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
	ConnectionURL string `mapstructure:"connection_url" validate:"required"`
}

var sampleConfig = `connection_url: "mongodb://admin:pass123@localhost:3306"`

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
	client   *mongo.Client
	excluded map[string]bool
	logger   log.Logger
	config   Config
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger) *Extractor {
	e := &Extractor{
		logger: logger,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	return e
}

func (e *Extractor) Init(ctx context.Context, config plugins.Config) error {
	if err := e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	// build excluded list
	e.buildExcludedCollections()

	// setup client
	var err error
	e.client, err = createAndConnnectClient(ctx, e.config.ConnectionURL)
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}

	return nil
}

// Extract collects metadata of each database through emitter
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	databases, err := e.client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("list database names: %w", err)
	}

	for _, dbName := range databases {
		database := e.client.Database(dbName)
		if err := e.extractCollections(ctx, database, emit); err != nil {
			return fmt.Errorf("extract collections: %w", err)
		}
	}

	return nil
}

// Extract and output collections from a single mongo database
func (e *Extractor) extractCollections(ctx context.Context, db *mongo.Database, emit plugins.Emit) error {
	collections, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("list collection names: %w", err)
	}

	// we need to sort the collections for testing purpose
	// this ensures the returned collection list are in consistent order
	// or else test might fail
	sort.Strings(collections)
	for _, collectionName := range collections {
		// skip if collection is default mongo
		if e.isDefaultCollection(collectionName) {
			continue
		}

		table, err := e.buildTable(ctx, db, collectionName)
		if err != nil {
			return fmt.Errorf("build table: %w", err)
		}

		emit(models.NewRecord(table))
	}

	return nil
}

// Build table metadata model from a collection
func (e *Extractor) buildTable(ctx context.Context, db *mongo.Database, collectionName string) (*v1beta2.Asset, error) {
	// get total rows
	totalRows, err := db.Collection(collectionName).EstimatedDocumentCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetch total no of rows: %w", err)
	}
	data, err := anypb.New(&v1beta2.Table{
		Profile: &v1beta2.TableProfile{
			TotalRows: totalRows,
		},
		Attributes: &structpb.Struct{}, // ensure attributes don't get overwritten if present
	})
	if err != nil {
		return nil, fmt.Errorf("build Any struct: %w", err)
	}

	return &v1beta2.Asset{
		Urn:     models.NewURN("mongodb", e.UrnScope, "collection", fmt.Sprintf("%s.%s", db.Name(), collectionName)),
		Name:    collectionName,
		Service: "mongodb",
		Type:    "table",
		Data:    data,
	}, nil
}

// Build a map of excluded collections using list of collection names
func (e *Extractor) buildExcludedCollections() {
	excluded := make(map[string]bool)
	for _, collection := range defaultCollections {
		excluded[collection] = true
	}

	e.excluded = excluded
}

// Check if collection is default using stored map
func (e *Extractor) isDefaultCollection(collectionName string) bool {
	_, ok := e.excluded[collectionName]
	return ok
}

// Create mongo client and tries to connect
func createAndConnnectClient(ctx context.Context, uri string) (*mongo.Client, error) {
	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.NewClient(clientOptions)
	if err != nil {
		return nil, err
	}
	if err := client.Connect(ctx); err != nil {
		return nil, err
	}

	return client, nil
}

func init() {
	if err := registry.Extractors.Register("mongodb", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
