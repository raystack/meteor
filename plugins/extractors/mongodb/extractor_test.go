//+build integration

package mongodb_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins/extractors/mongodb"
	"github.com/odpf/meteor/plugins/testutils"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	client *mongo.Client
	testDB = "MeteorMongoExtractorTest"
	user   = "user"
	pass   = "abcd"
	ctx    = context.TODO()
)

func TestMain(m *testing.M) {
	// setup test
	opts := dockertest.RunOptions{
		Repository: "mongo",
		Tag:        "4.4.6",
		Env: []string{
			"MONGO_INITDB_ROOT_USERNAME=" + user,
			"MONGO_INITDB_ROOT_PASSWORD=" + pass,
		},
		ExposedPorts: []string{"27017"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"27017": {
				{HostIP: "0.0.0.0", HostPort: "27017"},
			},
		},
	}
	retryFn := func(resource *dockertest.Resource) (err error) {
		uri := fmt.Sprintf("mongodb://%s:%s@%s", user, pass, "127.0.0.1:27017")
		clientOptions := options.Client().ApplyURI(uri)
		client, err = mongo.NewClient(clientOptions)
		if err != nil {
			return
		}
		err = client.Connect(ctx)
		if err != nil {
			return
		}

		return client.Ping(ctx, nil)
	}
	err, purgeFn := testutils.CreateContainer(opts, retryFn)
	if err != nil {
		log.Fatal(err)
	}

	if err := setup(); err != nil {
		log.Fatal(err)
	}

	// run tests
	code := m.Run()

	// clean tests
	if err := client.Disconnect(ctx); err != nil {
		log.Fatal(err)
	}
	if err := purgeFn(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}
func TestExtract(t *testing.T) {
	t.Run("should return error if no user_id in config", func(t *testing.T) {
		extr := new(mongodb.Extractor)
		_, err := extr.Extract(map[string]interface{}{
			"password": "abcd",
			"host":     "127.0.0.1:27017",
		})

		assert.Equal(t, extractor.InvalidConfigError{}, err)
	})

	t.Run("should return error if no password in config", func(t *testing.T) {
		extr := new(mongodb.Extractor)
		_, err := extr.Extract(map[string]interface{}{
			"user_id": "Gaurav_Ubuntu",
			"host":    "127.0.0.1:27017",
		})

		assert.Equal(t, extractor.InvalidConfigError{}, err)
	})

	t.Run("should return error if no host in config", func(t *testing.T) {
		extr := new(mongodb.Extractor)
		_, err := extr.Extract(map[string]interface{}{
			"user_id":  "user",
			"password": "abcd",
		})

		assert.Equal(t, extractor.InvalidConfigError{}, err)
	})

	t.Run("should return mockdata we generated with mongo running on 127.0.0.1", func(t *testing.T) {
		extractor := new(mongodb.Extractor)
		result, err := extractor.Extract(map[string]interface{}{
			"user_id":  user,
			"password": pass,
			"host":     "127.0.0.1:27017",
		})
		if err != nil {
			t.Fatal(err)
		}
		expected := getExpectedVal()
		assert.Equal(t, expected, result)
	})
}

func getExpectedVal() []meta.Table {
	return []meta.Table{
		{
			Urn:  testDB + "." + "connection",
			Name: "connection",
			Profile: &meta.TableProfile{
				TotalRows: 3,
			},
		},
		{
			Urn:  testDB + "." + "posts",
			Name: "posts",
			Profile: &meta.TableProfile{
				TotalRows: 3,
			},
		},
		{
			Urn:  testDB + "." + "reach",
			Name: "reach",
			Profile: &meta.TableProfile{
				TotalRows: 3,
			},
		},
	}
}

func setup() (err error) {
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
	return
}

func insertPosts(ctx context.Context, client *mongo.Client) (err error) {
	collection := client.Database(testDB).Collection("posts")
	_, insertErr := collection.InsertMany(ctx, []interface{}{
		bson.D{{"title", "World"}, {"body", "Hello World"}},
		bson.D{{"title", "Mars"}, {"body", "Hello Mars"}},
		bson.D{{"title", "Pluto"}, {"body", "Hello Pluto"}},
	})
	if insertErr != nil {
		return insertErr
	}
	return
}

func insertConnections(ctx context.Context, client *mongo.Client) (err error) {
	collection := client.Database(testDB).Collection("connection")
	_, insertErr := collection.InsertMany(ctx, []interface{}{
		bson.D{{"name", "Albert"}, {"relation", "mutual"}},
		bson.D{{"name", "Josh"}, {"relation", "following"}},
		bson.D{{"name", "Abish"}, {"relation", "follower"}},
	})
	if insertErr != nil {
		return insertErr
	}
	return
}

func insertReach(ctx context.Context, client *mongo.Client) (err error) {
	collection := client.Database(testDB).Collection("reach")
	_, insertErr := collection.InsertMany(ctx, []interface{}{
		bson.D{{"views", "500"}, {"likes", "200"}, {"comments", "50"}},
		bson.D{{"views", "400"}, {"likes", "100"}, {"comments", "5"}},
		bson.D{{"views", "800"}, {"likes", "300"}, {"comments", "80"}},
	})
	if insertErr != nil {
		return insertErr
	}
	return
}
