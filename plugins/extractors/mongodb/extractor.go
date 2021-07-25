package mongodb

import (
	"context"
	"fmt"
	"sort"

	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var defaultCollections = map[string]bool{
	"system.users":    true,
	"system.version":  true,
	"system.sessions": true,
}

type Config struct {
	UserID   string `mapstructure:"user_id" validate:"required"`
	Password string `mapstructure:"password" validate:"required"`
	Host     string `mapstructure:"host" validate:"required"`
}

type Extractor struct {
	logger plugins.Logger
}

func (e *Extractor) Extract(ctx context.Context, configMap map[string]interface{}, out chan<- interface{}) (err error) {
	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return extractor.InvalidConfigError{}
	}

	uri := "mongodb://" + config.UserID + ":" + config.Password + "@" + config.Host
	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.NewClient(clientOptions)
	if err != nil {
		return
	}
	err = client.Connect(ctx)
	if err != nil {
		return
	}
	result, err := e.listCollections(client, ctx)
	if err != nil {
		return
	}
	out <- result
	return nil
}

func (e *Extractor) listCollections(client *mongo.Client, ctx context.Context) (result []meta.Table, err error) {
	databases, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return
	}
	sort.Strings(databases)
	var collections []string
	for _, db_name := range databases {
		db := client.Database(db_name)
		collections, err = db.ListCollectionNames(ctx, bson.D{})
		if err != nil {
			return
		}
		sort.Strings(collections)
		for _, collection_name := range collections {
			if e.collectionIsDefault(collection_name) {
				continue
			}

			count, err := db.Collection(collection_name).EstimatedDocumentCount(ctx)
			if err != nil {
				fmt.Println(count)
				return result, err
			}
			result = append(result, meta.Table{
				Urn:  fmt.Sprintf("%s.%s", db_name, collection_name),
				Name: collection_name,
				Profile: &meta.TableProfile{
					TotalRows: count,
				},
			})
		}
	}
	return result, err
}

func (e *Extractor) collectionIsDefault(collectionName string) bool {
	isDefault, ok := defaultCollections[collectionName]
	if !ok {
		return false
	}

	return isDefault
}

func init() {
	if err := extractor.Catalog.Register("mongodb", &Extractor{
		logger: plugins.Log,
	}); err != nil {
		panic(err)
	}
}
