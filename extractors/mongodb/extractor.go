package mongodb

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/mitchellh/mapstructure"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Extractor struct{}

type Config struct {
	UserID   string `mapstructure:"user_id"`
	Password string `mapstructure:"password"`
	Host     string `mapstructure:"host"`
}

func (e *Extractor) Extract(configMap map[string]interface{}) (result []map[string]interface{}, err error) {
	config, err := e.getConfig(configMap)
	if err != nil {
		return
	}
	err = e.validateConfig(config)
	if err != nil {
		return
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

func (e *Extractor) listCollections(client *mongo.Client, ctx context.Context) (result []map[string]interface{}, err error) {
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
		for _, collection := range collections {
			row := make(map[string]interface{})
			row["collection_name"] = collection
			row["database_name"] = db_name
			count, _ := db.Collection(collection).EstimatedDocumentCount(ctx)
			row["document_count"] = int(count)
			result = append(result, row)
		}
	}
	return result, err
}

func (e *Extractor) getConfig(configMap map[string]interface{}) (config Config, err error) {
	err = mapstructure.Decode(configMap, &config)
	return
}

func (e *Extractor) validateConfig(config Config) (err error) {
	if config.UserID == "" {
		return errors.New("user_id is required")
	}
	if config.Password == "" {
		return errors.New("password is required")
	}
	if config.Host == "" {
		return errors.New("host address is required")
	}
	return
}
