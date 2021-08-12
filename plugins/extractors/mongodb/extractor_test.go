//+build integration

package mongodb_test

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/odpf/meteor/internal/logger"
	"github.com/odpf/meteor/plugins"
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
)

func TestMain(m *testing.M) {
	ctx := context.TODO()

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

	if err := setup(ctx); err != nil {
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
		err := newExtractor().Extract(context.TODO(), map[string]interface{}{
			"password": "abcd",
			"host":     "127.0.0.1:27017",
		}, make(chan interface{}))

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})

	t.Run("should return error if no password in config", func(t *testing.T) {
		err := newExtractor().Extract(context.TODO(), map[string]interface{}{
			"user_id": "Gaurav_Ubuntu",
			"host":    "127.0.0.1:27017",
		}, make(chan interface{}))

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})

	t.Run("should return error if no host in config", func(t *testing.T) {
		err := newExtractor().Extract(context.TODO(), map[string]interface{}{
			"user_id":  "user",
			"password": "abcd",
		}, make(chan interface{}))

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})

	t.Run("should extract and output tables metadata along with its columns", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		out := make(chan interface{})

		go func() {
			err := newExtractor().Extract(ctx, map[string]interface{}{
				"user_id":  user,
				"password": pass,
				"host":     "127.0.0.1:27017",
			}, out)
			close(out)

			assert.Nil(t, err)
		}()

		var results []meta.Table
		for d := range out {
			table, ok := d.(meta.Table)
			if !ok {
				t.Fatal(errors.New("invalid table format"))
			}

			results = append(results, table)
		}

		assert.Equal(t, getExpected(), results)
	})
}

func setup(ctx context.Context) (err error) {
	// create and populate connections collection
	err = createCollection(ctx, "connections", []interface{}{
		bson.D{{Key: "name", Value: "Albert"}, {Key: "relation", Value: "mutual"}},
		bson.D{{Key: "name", Value: "Josh"}, {Key: "relation", Value: "following"}},
		bson.D{{Key: "name", Value: "Abish"}, {Key: "relation", Value: "follower"}},
	})
	if err != nil {
		return
	}

	// create and populate posts collection
	err = createCollection(ctx, "posts", []interface{}{
		bson.D{{Key: "title", Value: "World"}, {Key: "body", Value: "Hello World"}},
		bson.D{{Key: "title", Value: "Mars"}, {Key: "body", Value: "Hello Mars"}},
	})
	if err != nil {
		return
	}

	// create and populate stats collection
	err = createCollection(ctx, "stats", []interface{}{
		bson.D{{Key: "views", Value: "500"}, {Key: "likes", Value: "200"}},
	})
	if err != nil {
		return
	}

	return
}

func createCollection(ctx context.Context, collection_name string, data []interface{}) (err error) {
	collection := client.Database(testDB).Collection(collection_name)
	_, err = collection.InsertMany(ctx, data)
	return
}

func newExtractor() *mongodb.Extractor {
	return mongodb.New(
		logger.NewWithWriter("info", ioutil.Discard),
	)
}

func getExpected() []meta.Table {
	return []meta.Table{
		{
			Urn:  testDB + ".connections",
			Name: "connections",
			Profile: &meta.TableProfile{
				TotalRows: 3,
			},
		},
		{
			Urn:  testDB + ".posts",
			Name: "posts",
			Profile: &meta.TableProfile{
				TotalRows: 2,
			},
		},
		{
			Urn:  testDB + ".stats",
			Name: "stats",
			Profile: &meta.TableProfile{
				TotalRows: 1,
			},
		},
	}
}
