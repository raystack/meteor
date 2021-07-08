package mongodb_test

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/odpf/meteor/pkg/extractors/mongodb"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var testDB string = "MeteorMongoExtractorTest"

var posts = []interface{}{
	bson.D{{"title", "World"}, {"body", "Hello World"}},
	bson.D{{"title", "Mars"}, {"body", "Hello Mars"}},
	bson.D{{"title", "Pluto"}, {"body", "Hello Pluto"}},
}

var connections = []interface{}{
	bson.D{{"name", "Albert"}, {"relation", "mutual"}},
	bson.D{{"name", "Josh"}, {"relation", "following"}},
	bson.D{{"name", "Abish"}, {"relation", "follower"}},
}

var reach = []interface{}{
	bson.D{{"views", "500"}, {"likes", "200"}, {"comments", "50"}},
	bson.D{{"views", "400"}, {"likes", "100"}, {"comments", "5"}},
	bson.D{{"views", "800"}, {"likes", "300"}, {"comments", "80"}},
}

func TestExtract(t *testing.T) {
	t.Run("should return error if no user_id in config", func(t *testing.T) {
		extractor := new(mongodb.Extractor)
		_, err := extractor.Extract(map[string]interface{}{
			"password": "abcd",
			"host":     "localhost:27017",
		})

		assert.NotNil(t, err)
	})

	t.Run("should return error if no password in config", func(t *testing.T) {
		extractor := new(mongodb.Extractor)
		_, err := extractor.Extract(map[string]interface{}{
			"user_id": "Gaurav_Ubuntu",
			"host":    "localhost:27017",
		})

		assert.NotNil(t, err)
	})

	t.Run("should return error if no host in config", func(t *testing.T) {
		extractor := new(mongodb.Extractor)
		_, err := extractor.Extract(map[string]interface{}{
			"user_id":  "user",
			"password": "abcd",
		})

		assert.NotNil(t, err)
	})

	t.Run("should return mockdata we generated with mongo running on localhost", func(t *testing.T) {
		extractor := new(mongodb.Extractor)
		uri := "mongodb://user:abcd@localhost:27017"
		clientOptions := options.Client().ApplyURI(uri)

		err := mockDataGenerator(clientOptions)
		if err != nil {
			t.Fatal(err)
		}
		result, err := extractor.Extract(map[string]interface{}{
			"user_id":  "user",
			"password": "abcd",
			"host":     "localhost:27017",
		})
		if err != nil {
			t.Fatal(err)
		}
		expected := getExpectedVal()
		assert.Equal(t, result, expected)
	})
}

func getExpectedVal() (expected []map[string]interface{}) {
	expected = []map[string]interface{}{
		{
			"collection_name": "connection",
			"database_name":   testDB,
			"document_count":  3,
		},
		{
			"collection_name": "posts",
			"database_name":   testDB,
			"document_count":  3,
		},
		{
			"collection_name": "reach",
			"database_name":   testDB,
			"document_count":  3,
		},
		{
			"collection_name": "system.users",
			"database_name":   "admin",
			"document_count":  1,
		},
		{
			"collection_name": "system.version",
			"database_name":   "admin",
			"document_count":  2,
		},
		{
			"collection_name": "system.sessions",
			"database_name":   "config",
			"document_count":  0,
		},
	}
	return
}

func mockDataGenerator(clientOptions *options.ClientOptions) (err error) {
	client, err := mongo.NewClient(clientOptions)
	if err != nil {
		log.Fatal(err)
		return
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		return
	}
	db := client.Database("local")
	_ = db.Collection("startup_log").Drop(ctx)
	db = client.Database(testDB)
	_ = db.Drop(ctx)
	err = insertPosts(ctx, client)
	if err != nil {
		return
	}
	err = insertConnections(ctx, client)
	if err != nil {
		return
	}
	err = insertReach(ctx, client)
	if err != nil {
		return
	}
	client.Disconnect(ctx)
	return
}

func insertPosts(ctx context.Context, client *mongo.Client) (err error) {
	collection := client.Database(testDB).Collection("posts")
	_, insertErr := collection.InsertMany(ctx, posts)
	if insertErr != nil {
		return insertErr
	}
	return
}

func insertConnections(ctx context.Context, client *mongo.Client) (err error) {
	collection := client.Database(testDB).Collection("connection")
	_, insertErr := collection.InsertMany(ctx, connections)
	if insertErr != nil {
		return insertErr
	}
	return
}

func insertReach(ctx context.Context, client *mongo.Client) (err error) {
	collection := client.Database(testDB).Collection("reach")
	_, insertErr := collection.InsertMany(ctx, reach)
	if insertErr != nil {
		return insertErr
	}
	return
}
