package mongodb

import (
	"context"
	"errors"
	"time"

	"github.com/mitchellh/mapstructure"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Extractor struct{}

type Result struct {
	CollectionName string
	DatabaseName   string
	Indexes        []bson.D
}

type Config struct {
	UserID   string `mapstructure:"user_id"`
	Password string `mapstructure:"password"`
	Host     string `mapstructure:"host"`
}

func (e *Extractor) Extract(configMap map[string]interface{}) (result []Result, err error) {
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
	result, err = e.listCollections(clientOptions)
	if err != nil {
		return
	}
	return result, err
}

func (e *Extractor) listCollections(clientOptions *options.ClientOptions) (result []Result, err error) {
	client, err := mongo.NewClient(clientOptions)
	if err != nil {
		return
	}
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		return
	}
	databases, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return
	}
	var collections []string
	for _, db_name := range databases {
		db := client.Database(db_name)
		collections, err = db.ListCollectionNames(ctx, bson.D{})
		if err != nil {
			return
		}
		for _, collection := range collections {
			var row Result
			row.CollectionName = collection
			row.DatabaseName = db_name
			row.Indexes = e.listIndexes(clientOptions, collection, db_name)
			result = append(result, row)
		}
	}
	return result, err
}

func (e *Extractor) listIndexes(clientOptions *options.ClientOptions, collection string, db_name string) (results []bson.D) {
	client, err := mongo.NewClient(clientOptions)
	if err != nil {
		return
	}
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		return
	}
	db := client.Database(db_name)
	iv := db.Collection(collection).Indexes()
	cur, err := iv.List(ctx)
	if err != nil {
		return
	}
	if err := cur.All(context.TODO(), &results); err != nil {
		return
	}
	return
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
