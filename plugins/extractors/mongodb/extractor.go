package mongodb

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins/utils"
	"github.com/odpf/meteor/proto/odpf/meta"
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

type Extractor struct{}

func New() extractor.TableExtractor {
	return &Extractor{}
}

func (e *Extractor) Extract(configMap map[string]interface{}) (result []meta.Table, err error) {
	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return result, extractor.InvalidConfigError{}
	}

	uri := "mongodb://" + config.UserID + ":" + config.Password + "@" + config.Host
	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.NewClient(clientOptions)
	if err != nil {
		return
	}
	ctx, _ := context.WithTimeout(context.Background(), 4*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		return
	}
	result, err = e.listCollections(client, ctx)
	if err != nil {
		return
	}
	return result, err
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
