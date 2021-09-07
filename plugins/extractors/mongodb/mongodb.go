package mongodb

import (
	"context"
	_ "embed"
	"fmt"
	"sort"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/assets"
	"github.com/odpf/meteor/proto/odpf/assets/common"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
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

type Config struct {
	UserID   string `mapstructure:"user_id" validate:"required"`
	Password string `mapstructure:"password" validate:"required"`
	Host     string `mapstructure:"host" validate:"required"`
}

var sampleConfig = `
 host: localhost:27017
 user_id: admin
 password: 1234`

type Extractor struct {
	// internal states
	out      chan<- interface{}
	client   *mongo.Client
	excluded map[string]bool

	// dependencies
	logger log.Logger
}

func New(logger log.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}

func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description:  "Collection metadata from MongoDB Server",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"oss,extractor"},
	}
}

func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

func (e *Extractor) Extract(ctx context.Context, configMap map[string]interface{}, out chan<- interface{}) (err error) {
	e.out = out

	// build config
	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	// build excluded list
	e.buildExcludedCollections()

	// setup client
	uri := fmt.Sprintf("mongodb://%s:%s@%s", config.UserID, config.Password, config.Host)
	client, err := createAndConnnectClient(context.Background(), uri)
	if err != nil {
		return
	}
	e.client = client

	return e.extract(ctx)
}

// Extract and output collections from all databases found
func (e *Extractor) extract(ctx context.Context) (err error) {
	databases, err := e.client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return
	}

	for _, db_name := range databases {
		database := e.client.Database(db_name)
		if err := e.extractCollections(ctx, database); err != nil {
			return err
		}
	}
	return err
}

// Extract and output collections from a single mongo database
func (e *Extractor) extractCollections(ctx context.Context, db *mongo.Database) (err error) {
	collections, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return
	}

	// we need to sort the collections for testing purpose
	// this ensures the returned collection list are in consistent order
	// or else test might fail
	sort.Strings(collections)
	for _, collection_name := range collections {
		// skip if collection is default mongo
		if e.isDefaultCollection(collection_name) {
			continue
		}

		table, err := e.buildTable(ctx, db, collection_name)
		if err != nil {
			return err
		}

		e.out <- table
	}

	return
}

// Build table metadata model from a collection
func (e *Extractor) buildTable(ctx context.Context, db *mongo.Database, collection_name string) (table assets.Table, err error) {
	// get total rows
	total_rows, err := db.Collection(collection_name).EstimatedDocumentCount(ctx)
	if err != nil {
		return
	}

	table = assets.Table{
		Resource: &common.Resource{
			Urn:  fmt.Sprintf("%s.%s", db.Name(), collection_name),
			Name: collection_name,
		},
		Profile: &assets.TableProfile{
			TotalRows: total_rows,
		},
	}

	return
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
