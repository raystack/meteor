package bigtable

import (
	"cloud.google.com/go/bigtable"
	"context"
	"errors"
	"github.com/odpf/meteor/plugins/extractors/bigtable/mocks"
	"github.com/stretchr/testify/mock"
	"io/ioutil"
	"os"
	"testing"

	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/logger"
	"github.com/stretchr/testify/assert"
)

var log = logger.NewWithWriter("info", ioutil.Discard)

func TestExtract(t *testing.T) {
	t.Run("should return error if no project_id in config", func(t *testing.T) {
		extr := New(log)
		_, err := extr.Extract(map[string]interface{}{
			"wrong-config": "sample-project",
		})

		assert.Equal(t, extractor.InvalidConfigError{}, err)
	})

	t.Run("should return error if project_id is empty", func(t *testing.T) {
		extr := New(log)
		_, err := extr.Extract(map[string]interface{}{
			"project_id": "",
		})

		assert.EqualError(t, err, "invalid extractor config")
	})

	t.Run("should return bigtable metadata on success", func(t *testing.T) {
		extr := New(log)
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

		result, err := extr.Extract(map[string]interface{}{
			"project_id": "dev",
		})
		assert.Nil(t, err)
		assert.Equal(t, 1, len(result))
		assert.Equal(t, "dev.dev.records", result[0].Urn)
		assert.Equal(t, "bigtable", result[0].Source)
		assert.Equal(t, "records", result[0].Name)
		assert.Equal(t, 1, len(result[0].Custom.CustomProperties))
		os.Unsetenv("BIGTABLE_EMULATOR_HOST")
	})

	t.Run("should handle instance admin client initialization error", func(t *testing.T) {
		extr := New(log)
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

		result, err := extr.Extract(map[string]interface{}{
			"project_id": "dev",
		})
		assert.EqualError(t, err, "random error")
		assert.Nil(t, result)
		os.Unsetenv("BIGTABLE_EMULATOR_HOST")
	})

	t.Run("should handle errors in getting instances list", func(t *testing.T) {
		extr := New(log)
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

		result, err := extr.Extract(map[string]interface{}{
			"project_id": "dev",
		})
		assert.EqualError(t, err, "random error")
		assert.Nil(t, result)
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
