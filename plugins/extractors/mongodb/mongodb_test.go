//go:build plugins
// +build plugins

package mongodb_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/extractors/mongodb"
	"github.com/raystack/meteor/test/mocks"
	"github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	testDB   = "MeteorMongoExtractorTest"
	user     = "user"
	pass     = "abcd"
	urnScope = "test-mongodb"
)

var (
	host            string
	client          *mongo.Client
	dockerAvailable bool
)

func TestMain(m *testing.M) {
	dockerAvailable = utils.CheckDockerAvailability()
	if !dockerAvailable {
		os.Exit(m.Run())
	}

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
	}
	retryFn := func(resource *dockertest.Resource) (err error) {
		host = resource.GetHostPort("27017/tcp")
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
	purgeFn, err := utils.CreateContainer(opts, retryFn)
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

func TestInit(t *testing.T) {
	utils.SkipIfNoDocker(t, dockerAvailable)
	t.Run("should return error for invalid", func(t *testing.T) {
		err := mongodb.New(utils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"invalid_config": "invalid_config_value",
			}})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
}

func TestExtract(t *testing.T) {
	utils.SkipIfNoDocker(t, dockerAvailable)
	t.Run("should extract and output tables metadata along with its columns", func(t *testing.T) {
		ctx := context.TODO()
		extr := mongodb.New(utils.Logger)

		err := extr.Init(ctx, plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"connection_url": fmt.Sprintf("mongodb://%s:%s@%s", user, pass, host),
			}})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)

		assert.NoError(t, err)
		utils.AssertEqualProtos(t, getExpected(t), emitter.GetAllEntities())
	})
}

func setup(ctx context.Context) (err error) {
	// create and populate connections collection
	err = createCollection(ctx, "connections", []any{
		bson.D{{Key: "name", Value: "Albert"}, {Key: "relation", Value: "mutual"}},
		bson.D{{Key: "name", Value: "Josh"}, {Key: "relation", Value: "following"}},
		bson.D{{Key: "name", Value: "Abish"}, {Key: "relation", Value: "follower"}},
	})
	if err != nil {
		return
	}

	// create and populate posts collection
	err = createCollection(ctx, "posts", []any{
		bson.D{{Key: "title", Value: "World"}, {Key: "body", Value: "Hello World"}},
		bson.D{{Key: "title", Value: "Mars"}, {Key: "body", Value: "Hello Mars"}},
	})
	if err != nil {
		return
	}

	// create and populate stats collection
	err = createCollection(ctx, "stats", []any{
		bson.D{{Key: "views", Value: "500"}, {Key: "likes", Value: "200"}},
	})
	if err != nil {
		return
	}

	return
}

func createCollection(ctx context.Context, collectionName string, data []any) (err error) {
	collection := client.Database(testDB).Collection(collectionName)
	_, err = collection.InsertMany(ctx, data)
	return
}

func getExpected(t *testing.T) []*meteorv1beta1.Entity {
	return []*meteorv1beta1.Entity{
		models.NewEntity("urn:mongodb:test-mongodb:collection:"+testDB+".connections", "table", "connections", "mongodb", map[string]any{
			"profile": map[string]any{
				"total_rows": float64(3),
			},
		}),
		models.NewEntity("urn:mongodb:test-mongodb:collection:"+testDB+".posts", "table", "posts", "mongodb", map[string]any{
			"profile": map[string]any{
				"total_rows": float64(2),
			},
		}),
		models.NewEntity("urn:mongodb:test-mongodb:collection:"+testDB+".stats", "table", "stats", "mongodb", map[string]any{
			"profile": map[string]any{
				"total_rows": float64(1),
			},
		}),
	}
}
