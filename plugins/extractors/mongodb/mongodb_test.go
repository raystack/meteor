//+build integration

package mongodb_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/mongodb"
	"github.com/odpf/meteor/proto/odpf/assets"
	"github.com/odpf/meteor/proto/odpf/assets/common"
	"github.com/odpf/meteor/test"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	testDB = "MeteorMongoExtractorTest"
	user   = "user"
	pass   = "abcd"
	port   = "27017"
)

var (
	host   = "127.0.0.1:" + port
	client *mongo.Client
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
		ExposedPorts: []string{port},
		PortBindings: map[docker.Port][]docker.PortBinding{
			port: {
				{HostIP: "0.0.0.0", HostPort: port},
			},
		},
	}
	retryFn := func(resource *dockertest.Resource) (err error) {
		uri := fmt.Sprintf("mongodb://%s:%s@%s", user, pass, host)
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
	purgeFn, err := test.CreateContainer(opts, retryFn)
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
	t.Run("should return error for invalid", func(t *testing.T) {
		err := newExtractor().Extract(context.TODO(), map[string]interface{}{
			"password": pass,
			"host":     host,
		}, make(chan interface{}))

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})

	t.Run("should extract and output tables metadata along with its columns", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		extractOut := make(chan interface{})

		go func() {
			err := newExtractor().Extract(ctx, map[string]interface{}{
				"user_id":  user,
				"password": pass,
				"host":     host,
			}, extractOut)
			close(extractOut)

			assert.Nil(t, err)
		}()

		var results []assets.Table
		for d := range extractOut {
			table, ok := d.(assets.Table)
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

func createCollection(ctx context.Context, collectionName string, data []interface{}) (err error) {
	collection := client.Database(testDB).Collection(collectionName)
	_, err = collection.InsertMany(ctx, data)
	return
}

func newExtractor() *mongodb.Extractor {
	return mongodb.New(test.Logger)
}

func getExpected() []assets.Table {
	return []assets.Table{
		{
			Resource: &common.Resource{
				Urn:  testDB + ".connections",
				Name: "connections",
			},
			Profile: &assets.TableProfile{
				TotalRows: 3,
			},
		},
		{
			Resource: &common.Resource{
				Urn:  testDB + ".posts",
				Name: "posts",
			},
			Profile: &assets.TableProfile{
				TotalRows: 2,
			},
		},
		{
			Resource: &common.Resource{
				Urn:  testDB + ".stats",
				Name: "stats",
			},
			Profile: &assets.TableProfile{
				TotalRows: 1,
			},
		},
	}
}
