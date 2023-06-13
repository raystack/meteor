//go:build plugins
// +build plugins

package bigtable_test

import (
	"context"
	"errors"
	"testing"

	"cloud.google.com/go/bigtable"
	"github.com/goto/meteor/plugins"
	bt "github.com/goto/meteor/plugins/extractors/bigtable"
	btMocks "github.com/goto/meteor/plugins/extractors/bigtable/mocks"
	"github.com/goto/meteor/test/mocks"
	"github.com/goto/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	urnScope = "test-bigtable"
)

func TestInit(t *testing.T) {
	t.Run("should return error if no project_id in config", func(t *testing.T) {
		err := bt.New(utils.Logger, nil, nil).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"wrong-config": "sample-project",
			},
		})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should return error if project_id is empty", func(t *testing.T) {
		err := bt.New(utils.Logger, nil, nil).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"project_id": "",
			},
		})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should return error if service_account_base64 config is invalid", func(t *testing.T) {
		extr := bt.New(utils.Logger, nil, nil)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := extr.Init(ctx, plugins.Config{
			URNScope: "test-bigtable",
			RawConfig: map[string]interface{}{
				"project_id":             "google-project-id",
				"service_account_base64": "----", // invalid
			},
		})

		assert.ErrorContains(t, err, "decode Base64 encoded service account")
	})

	t.Run("should return error when failed to create instance admin client", func(t *testing.T) {
		expectedErr := errors.New("some error")
		mockInstanceAdminClient := func(ctx context.Context, cfg bt.Config) (bt.InstanceAdminClient, error) {
			return nil, expectedErr
		}

		extr := bt.New(utils.Logger, mockInstanceAdminClient, nil)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := extr.Init(ctx, plugins.Config{
			URNScope: "test-bigtable",
			RawConfig: map[string]interface{}{
				"project_id": "google-project-id",
			},
		})

		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("should return error when failed get instance info", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		expectedErr := errors.New("some error")
		mockInstanceAdminClient := func(ctx context.Context, cfg bt.Config) (bt.InstanceAdminClient, error) {
			mock := btMocks.NewInstanceAdminClient(t)
			mock.On("Instances", ctx).Return(nil, expectedErr)
			return mock, nil
		}

		extr := bt.New(utils.Logger, mockInstanceAdminClient, nil)

		err := extr.Init(ctx, plugins.Config{
			URNScope: "test-bigtable",
			RawConfig: map[string]interface{}{
				"project_id": "google-project-id",
			},
		})

		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("should return no error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockInstanceAdminClient := func(ctx context.Context, cfg bt.Config) (bt.InstanceAdminClient, error) {
			mock := btMocks.NewInstanceAdminClient(t)
			mock.On("Instances", ctx).Return([]*bigtable.InstanceInfo{
				{Name: "instance-A"},
			}, nil)
			return mock, nil
		}
		extr := bt.New(utils.Logger, mockInstanceAdminClient, nil)

		err := extr.Init(ctx, plugins.Config{
			URNScope: "test-bigtable",
			RawConfig: map[string]interface{}{
				"project_id": "google-project-id",
			},
		})

		assert.NoError(t, err)
	})
}

func TestExtract(t *testing.T) {
	t.Run("should return no error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockInstanceAdminClient := func(ctx context.Context, cfg bt.Config) (bt.InstanceAdminClient, error) {
			m := btMocks.NewInstanceAdminClient(t)
			m.On("Instances", ctx).Return([]*bigtable.InstanceInfo{
				{Name: "instance-A"},
			}, nil)
			return m, nil
		}

		mockAdminClient := func(ctx context.Context, instance string, config bt.Config) (bt.AdminClient, error) {
			m := btMocks.NewAdminClient(t)
			m.On("Tables", ctx).Return([]string{"table-a"}, nil)
			m.On("TableInfo", ctx, mock.Anything).Return(&bigtable.TableInfo{
				FamilyInfos: []bigtable.FamilyInfo{
					{Name: "family-a"},
				},
			}, nil)
			return m, nil
		}
		extr := bt.New(utils.Logger, mockInstanceAdminClient, mockAdminClient)

		err := extr.Init(ctx, plugins.Config{
			URNScope: "test-bigtable",
			RawConfig: map[string]interface{}{
				"project_id": "google-project-id",
			},
		})

		assert.NoError(t, err)

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)
		assert.NoError(t, err)

		actual := emitter.GetAllData()
		utils.AssertProtosWithJSONFile(t, "testdata/expected-assets.json", actual)
	})

	t.Run("should return error when failed to create instance admin client", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockInstanceAdminClient := func(ctx context.Context, cfg bt.Config) (bt.InstanceAdminClient, error) {
			m := btMocks.NewInstanceAdminClient(t)
			m.On("Instances", ctx).Return([]*bigtable.InstanceInfo{
				{Name: "instance-A"},
			}, nil)
			return m, nil
		}

		expectedErr := errors.New("some error")
		mockAdminClient := func(ctx context.Context, instance string, config bt.Config) (bt.AdminClient, error) {
			return nil, expectedErr
		}
		extr := bt.New(utils.Logger, mockInstanceAdminClient, mockAdminClient)

		err := extr.Init(ctx, plugins.Config{
			URNScope: "test-bigtable",
			RawConfig: map[string]interface{}{
				"project_id": "google-project-id",
			},
		})

		assert.NoError(t, err)

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)
		assert.ErrorIs(t, err, expectedErr)
		assert.Len(t, emitter.GetAllData(), 0)
	})

	t.Run("should return no error when failed get table info", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockInstanceAdminClient := func(ctx context.Context, cfg bt.Config) (bt.InstanceAdminClient, error) {
			m := btMocks.NewInstanceAdminClient(t)
			m.On("Instances", ctx).Return([]*bigtable.InstanceInfo{
				{Name: "instance-A"},
			}, nil)
			return m, nil
		}

		expectedErr := errors.New("some error")
		mockAdminClient := func(ctx context.Context, instance string, config bt.Config) (bt.AdminClient, error) {
			m := btMocks.NewAdminClient(t)
			m.On("Tables", ctx).Return([]string{"table-a"}, nil)
			m.On("TableInfo", ctx, mock.Anything).Return(nil, expectedErr)
			return m, nil
		}
		extr := bt.New(utils.Logger, mockInstanceAdminClient, mockAdminClient)

		err := extr.Init(ctx, plugins.Config{
			URNScope: "test-bigtable",
			RawConfig: map[string]interface{}{
				"project_id": "google-project-id",
			},
		})

		assert.NoError(t, err)

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)
		assert.NoError(t, err)

		assert.Len(t, emitter.GetAllData(), 0)
	})
}
