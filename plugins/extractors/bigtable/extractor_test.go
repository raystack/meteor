//+build integration

package bigtable

import (
	"context"
	"errors"
	"log"
	"os"
	"testing"

	"cloud.google.com/go/bigtable"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/bigtable/mocks"
	"github.com/odpf/meteor/plugins/testutils"
	"github.com/odpf/meteor/proto/odpf/entities/resources"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/meteor/registry"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	// setup test
	opts := dockertest.RunOptions{
		Repository: "shopify/bigtable-emulator",
		Env: []string{
			"BIGTABLE_EMULATOR_HOST=localhost:9035",
		},
		ExposedPorts: []string{"9035"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9035": {
				{HostIP: "0.0.0.0", HostPort: "9035"},
			},
		},
		Cmd: []string{"-cf", "dev.records.data,dev.records.metadata"},
	}
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	retryFn := func(resource *dockertest.Resource) (err error) {
		_, err = bigtable.NewAdminClient(context.Background(), "dev", "dev")
		return
	}
	err, purgeFn := testutils.CreateContainer(opts, retryFn)
	if err != nil {
		log.Fatal("", err)
	}

	// run tests
	code := m.Run()

	if err := purgeFn(); err != nil {
		log.Fatal("", err)
	}
	os.Exit(code)
}

func TestExtract(t *testing.T) {
	t.Run("should return error if no project_id in config", func(t *testing.T) {
		extr, _ := registry.Extractors.Get("bigtable")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		extractOut := make(chan interface{})

		err := extr.Extract(ctx, map[string]interface{}{
			"wrong-config": "sample-project",
		}, extractOut)

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})

	t.Run("should return error if project_id is empty", func(t *testing.T) {
		extr, _ := registry.Extractors.Get("bigtable")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		extractOut := make(chan interface{})

		err := extr.Extract(ctx, map[string]interface{}{
			"project_id": "",
		}, extractOut)

		assert.EqualError(t, err, "invalid extractor config")
	})

	t.Run("should return bigtable metadata on success", func(t *testing.T) {
		extr, _ := registry.Extractors.Get("bigtable")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		extractOut := make(chan interface{})

		os.Setenv("BIGTABLE_EMULATOR_HOST", "localhost:9035")
		oldInstanceAdminClientCreator := instanceAdminClientCreator
		oldInstanceInfoGetter := instanceInfoGetter

		defer func() {
			instanceAdminClientCreator = oldInstanceAdminClientCreator
			instanceInfoGetter = oldInstanceInfoGetter
		}()

		instanceAdminClientCreator = func(_ context.Context, _ Config) (*bigtable.InstanceAdminClient, error) {
			return nil, nil
		}

		instanceInfoGetter = func(_ context.Context, _ InstancesFetcher) (instanceNames []string, err error) {
			instanceNames = append(instanceNames, "dev")
			return
		}

		go func() {
			extr.Extract(ctx, map[string]interface{}{
				"project_id": "dev",
			}, extractOut)

		}()

		for val := range extractOut {
			result := val.([]resources.Table)
			assert.Equal(t, "bigtable", result[0].Source)
		}

		os.Unsetenv("BIGTABLE_EMULATOR_HOST")
	})

	t.Run("should handle instance admin client initialization error", func(t *testing.T) {
		extr, _ := registry.Extractors.Get("bigtable")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		extractOut := make(chan interface{})

		os.Setenv("BIGTABLE_EMULATOR_HOST", "localhost:9035")
		oldInstanceAdminClientCreator := instanceAdminClientCreator
		oldInstanceInfoGetter := instanceInfoGetter

		defer func() {
			instanceAdminClientCreator = oldInstanceAdminClientCreator
			instanceInfoGetter = oldInstanceInfoGetter
		}()

		instanceAdminClientCreator = func(_ context.Context, _ Config) (*bigtable.InstanceAdminClient, error) {
			return nil, errors.New("random error")
		}

		go func() {
			err := extr.Extract(ctx, map[string]interface{}{
				"project_id": "dev",
			}, extractOut)
			assert.EqualError(t, err, "random error")

		}()

		for val := range extractOut {
			result := val.([]resources.Table)
			assert.Nil(t, result)
		}

		os.Unsetenv("BIGTABLE_EMULATOR_HOST")
	})

	t.Run("should handle errors in getting instances list", func(t *testing.T) {

		extr, _ := registry.Extractors.Get("bigtable")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		extractOut := make(chan interface{})

		os.Setenv("BIGTABLE_EMULATOR_HOST", "localhost:9035")
		oldInstanceAdminClientCreator := instanceAdminClientCreator
		oldInstanceInfoGetter := instanceInfoGetter

		defer func() {
			instanceAdminClientCreator = oldInstanceAdminClientCreator
			instanceInfoGetter = oldInstanceInfoGetter
		}()

		instanceAdminClientCreator = func(_ context.Context, _ Config) (*bigtable.InstanceAdminClient, error) {
			return nil, nil
		}

		instanceInfoGetter = func(_ context.Context, _ InstancesFetcher) (instanceNames []string, err error) {
			return nil, errors.New("random error")
		}

		go func() {
			err := extr.Extract(ctx, map[string]interface{}{
				"project_id": "dev",
			}, extractOut)
			assert.EqualError(t, err, "random error")

		}()

		for val := range extractOut {
			result := val.([]resources.Table)
			assert.Nil(t, result)
		}

		os.Unsetenv("BIGTABLE_EMULATOR_HOST")
	})

	t.Run("should return instances list from bigtable", func(t *testing.T) {
		mockedInstanceFetcher := mocks.InstancesFetcher{}
		instance1Info := &bigtable.InstanceInfo{Name: "foo"}
		instance2Info := &bigtable.InstanceInfo{Name: "bar"}
		instances := []*bigtable.InstanceInfo{instance1Info, instance2Info}
		mockedInstanceFetcher.On("Instances", mock.Anything).Return(instances, nil).Once()

		result, err := getInstancesInfo(context.Background(), &mockedInstanceFetcher)
		assert.Equal(t, []string{"foo", "bar"}, result)
		assert.Nil(t, err)
	})

	t.Run("should handle error on fetching instances list from bigtable", func(t *testing.T) {
		mockedInstanceFetcher := mocks.InstancesFetcher{}
		mockedInstanceFetcher.On("Instances", mock.Anything).
			Return(nil, errors.New("random error")).Once()

		result, err := getInstancesInfo(context.Background(), &mockedInstanceFetcher)
		assert.EqualError(t, err, "random error")
		assert.Nil(t, result)
	})
}
