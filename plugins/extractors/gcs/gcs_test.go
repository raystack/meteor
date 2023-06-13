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
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/test/mocks"
	"github.com/goto/meteor/test/utils"
	slog "github.com/goto/salt/log"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var client *storage.Client

func TestMain(m *testing.M) {
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
		PortBindings: map[docker.Port][]docker.PortBinding{
			"4443": {
				{HostIP: "0.0.0.0", HostPort: "4443"},
			},
		},
	}
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	retryFn := func(resource *dockertest.Resource) error {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		client, err = storage.NewClient(ctx,
			option.WithEndpoint("http://localhost:4443/storage/v1/"),
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
	t.Run("should return error if no project_id in config", func(t *testing.T) {
		err := New(utils.Logger, createClient).Init(context.TODO(), plugins.Config{
			URNScope: "test",
			RawConfig: map[string]interface{}{
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
			RawConfig: map[string]interface{}{
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
			RawConfig: map[string]interface{}{
				"project_id": "google-project-id",
			},
		})

		assert.NoError(t, err)
	})
}

func TestExtract(t *testing.T) {
	t.Run("should return no error", func(t *testing.T) {
		extr := New(utils.Logger, mockClient)
		ctx := context.Background()
		err := extr.Init(ctx, plugins.Config{
			URNScope: "test-gcs",
			RawConfig: map[string]interface{}{
				"project_id":   "google-project-id",
				"extract_blob": "true",
			},
		})

		assert.NoError(t, err)

		emitter := mocks.NewEmitter()
		err = extr.Extract(context.TODO(), emitter.Push)
		assert.NoError(t, err)

		actual := emitter.GetAllData()

		// the emulator returning dynamic timestamps
		// replace them with static ones
		replaceWithStaticTimestamp(t, actual)

		utils.AssertProtosWithJSONFile(t, "testdata/expected-assets.json", actual)
	})
}

func replaceWithStaticTimestamp(t *testing.T, actual []*v1beta2.Asset) {
	b := new(v1beta2.Bucket)
	err := actual[0].Data.UnmarshalTo(b)
	assert.NoError(t, err)

	time, err := time.Parse(time.RFC3339, "2023-06-13T03:46:12.372974Z")
	assert.NoError(t, err)
	b.Blobs[0].CreateTime = timestamppb.New(time)
	b.Blobs[0].UpdateTime = timestamppb.New(time)

	actual[0].Data, err = anypb.New(b)
	assert.NoError(t, err)
}
