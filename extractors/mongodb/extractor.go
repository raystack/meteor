package mongodb

import (
	"context"
	"errors"
	"fmt"
	"log"
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
	uri := "mongodb://" + config.UserID + ":" + config.Password + "@localhost:27017"
	clientOptions := options.Client().ApplyURI(uri)
	collections, err := ListCollections(clientOptions)
	if err != nil {
		log.Fatal(err)
		return
	}
	ListIndexes(clientOptions, collections)
	return result, err
}

func ListCollections(clientOptions *options.ClientOptions) (collection []string, err error) {
	client, err := mongo.NewClient(clientOptions)
	if err != nil {
		return
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		return
	}

	db := client.Database("blog")
	collections, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Collections :-> ", collections)
	return collections, err
}

func ListIndexes(clientOptions *options.ClientOptions, collections []string) {
	client, err := mongo.NewClient(clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	db := client.Database("blog")
	for i := 0; i < len(collections); i++ {
		iv := db.Collection(collections[i]).Indexes()
		cur, err := iv.List(ctx)
		if err != nil {
			log.Fatal(err)
		}
		var results []bson.M
		if err := cur.All(context.TODO(), &results); err != nil {
			log.Fatal(err)
		}
		fmt.Println(results)
	}
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

	return
}
