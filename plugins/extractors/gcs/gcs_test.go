//go:build plugins
// +build plugins

package gcs

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/pkg/errors"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/test/mocks"
	"github.com/raystack/meteor/test/utils"
	slog "github.com/raystack/salt/observability/logger"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/structpb"
)

var (
	client          *storage.Client
	dockerAvailable bool
)

func TestMain(m *testing.M) {
	dockerAvailable = utils.CheckDockerAvailability()
	if !dockerAvailable {
		os.Exit(m.Run())
	}

	pwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	// setup test
	opts := dockertest.RunOptions{
		Repository: "fsouza/fake-gcs-server",
		Tag:        "1.45",
		Env:        []string{},
		Mounts: []string{
			fmt.Sprintf("%s/testdata:/data", pwd),
		},
		Cmd: []string{
			"-scheme", "http",
		},
		ExposedPorts: []string{"4443"},
		PortBindings: map[docker.Port][]docker.PortBinding{"4443": {{HostPort: "0"}}},
	}
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	retryFn := func(resource *dockertest.Resource) error {
		hostPort := resource.GetHostPort("4443/tcp")
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		client, err = storage.NewClient(ctx,
			option.WithEndpoint("http://"+hostPort+"/storage/v1/"),
			option.WithoutAuthentication(),
		)
		if err != nil {
			return err
		}
		it := client.Bucket("test-bucket").Objects(ctx, nil)
		_, err := it.Next()
		if !errors.Is(err, iterator.Done) {
			return err
		}
		return nil
	}
	purgeFn, err := utils.CreateContainer(opts, retryFn)
	if err != nil {
		log.Fatal(err)
	}

	// run tests
	code := m.Run()

	// clean tests
	client.Close()
	if err := purgeFn(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

func mockClient(context.Context, slog.Logger, Config) (*storage.Client, error) {
	return client, nil
}

func TestInit(t *testing.T) {
	utils.SkipIfNoDocker(t, dockerAvailable)
	t.Run("should return error if no project_id in config", func(t *testing.T) {
		err := New(utils.Logger, createClient).Init(context.TODO(), plugins.Config{
			URNScope: "test",
			RawConfig: map[string]any{
				"wrong-config": "sample-project",
			},
		})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should return error if service_account_base64 config is invalid", func(t *testing.T) {
		extr := New(utils.Logger, createClient)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := extr.Init(ctx, plugins.Config{
			URNScope: "test-gcs",
			RawConfig: map[string]any{
				"project_id":             "google-project-id",
				"service_account_base64": "----", // invalid
			},
		})

		assert.ErrorContains(t, err, "decode Base64 encoded service account")
	})

	t.Run("should return no error", func(t *testing.T) {
		extr := New(utils.Logger, mockClient)
		ctx := context.Background()
		err := extr.Init(ctx, plugins.Config{
			URNScope: "test-gcs",
			RawConfig: map[string]any{
				"project_id": "google-project-id",
			},
		})

		assert.NoError(t, err)
	})
}

func TestExtract(t *testing.T) {
	utils.SkipIfNoDocker(t, dockerAvailable)
	t.Run("should return no error", func(t *testing.T) {
		extr := New(utils.Logger, mockClient)
		ctx := context.Background()
		err := extr.Init(ctx, plugins.Config{
			URNScope: "test-gcs",
			RawConfig: map[string]any{
				"project_id":   "google-project-id",
				"extract_blob": "true",
			},
		})

		assert.NoError(t, err)

		emitter := mocks.NewEmitter()
		err = extr.Extract(context.TODO(), emitter.Push)
		assert.NoError(t, err)

		actual := emitter.GetAllEntities()

		// the emulator returning dynamic timestamps
		// replace them with static ones
		replaceWithStaticTimestamp(t, actual)

		utils.AssertProtosWithJSONFile(t, "testdata/expected-assets.json", actual)
	})
}

func replaceWithStaticTimestamp(t *testing.T, actual []*meteorv1beta1.Entity) {
	if len(actual) == 0 || actual[0].Properties == nil {
		return
	}
	props := actual[0].Properties.AsMap()
	blobs, ok := props["blobs"].([]any)
	if !ok || len(blobs) == 0 {
		return
	}
	blob, ok := blobs[0].(map[string]any)
	if !ok {
		return
	}
	staticTime := "2023-06-13T03:46:12.372974Z"
	blob["create_time"] = staticTime
	blob["update_time"] = staticTime
	blobs[0] = blob
	props["blobs"] = blobs

	// Reconstruct the properties struct
	newProps, err := structpb.NewStruct(props)
	assert.NoError(t, err)
	actual[0].Properties = newProps
}
