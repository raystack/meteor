package mongodb

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"
	"sort"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/plugins"
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

// Config hold the set of configuration for the extractor
type Config struct {
	UserID   string `mapstructure:"user_id" validate:"required"`
	Password string `mapstructure:"password" validate:"required"`
	Host     string `mapstructure:"host" validate:"required"`
}

var sampleConfig = `
 host: localhost:27017
 user_id: admin
 password: "1234"`

// Extractor manages the communication with the mongo server
type Extractor struct {
	// internal states
	out      chan<- models.Record
	client   *mongo.Client
	excluded map[string]bool
	logger   log.Logger
	config   Config
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}

// Info returns the brief information about the extractor
func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description:  "Collection metadata from MongoDB Server",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"oss,extractor"},
	}
}

// Validate validates the configuration of the extractor
func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

func (e *Extractor) Init(ctx context.Context, configMap map[string]interface{}) (err error) {
	err = utils.BuildConfig(configMap, &e.config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	// build excluded list
	e.buildExcludedCollections()

	// setup client
	uri := fmt.Sprintf("mongodb://%s:%s@%s", e.config.UserID, e.config.Password, e.config.Host)
	e.client, err = createAndConnnectClient(ctx, uri)
	if err != nil {
		return
	}

	return
}

// Extract extracts the data from the mongo server
// and outputs the data to the out channel
func (e *Extractor) Extract(ctx context.Context, emitter plugins.Emitter) (err error) {
	databases, err := e.client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return
	}

	for _, dbName := range databases {
		database := e.client.Database(dbName)
		if err := e.extractCollections(ctx, database, emitter); err != nil {
			return err
		}
	}

	return
}

// Extract and output collections from a single mongo database
func (e *Extractor) extractCollections(ctx context.Context, db *mongo.Database, emitter plugins.Emitter) (err error) {
	collections, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return
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
			return err
		}

		emitter.Emit(models.NewRecord(table))
	}

	return
}

// Build table metadata model from a collection
func (e *Extractor) buildTable(ctx context.Context, db *mongo.Database, collectionName string) (table *assets.Table, err error) {
	// get total rows
	totalRows, err := db.Collection(collectionName).EstimatedDocumentCount(ctx)
	if err != nil {
		return
	}

	table = &assets.Table{
		Resource: &common.Resource{
			Urn:  fmt.Sprintf("%s.%s", db.Name(), collectionName),
			Name: collectionName,
		},
		Profile: &assets.TableProfile{
			TotalRows: totalRows,
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
