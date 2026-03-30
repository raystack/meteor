//go:build plugins
// +build plugins

package frontier_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pkg/errors"
	frontierProto "github.com/raystack/frontier/proto/v1beta1"
	"github.com/raystack/meteor/models"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/sinks/frontier"
	testUtils "github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	validConfig = map[string]interface{}{
		"host": "frontier:80",
	}
	urnScope = "test-frontier"
)

func TestInit(t *testing.T) {
	t.Run("should return error if config is invalid", func(t *testing.T) {
		sink := frontier.New(new(mockClient), testUtils.Logger)
		err := sink.Init(context.TODO(), plugins.Config{RawConfig: map[string]interface{}{
			"host": "",
		}})
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should not return error if config is valid", func(t *testing.T) {
		var err error
		ctx := context.TODO()

		client := new(mockClient)
		client.On("Connect", ctx, validConfig["host"]).Return(nil)
		defer client.AssertExpectations(t)

		sink := frontier.New(client, testUtils.Logger)
		err = sink.Init(ctx, plugins.Config{URNScope: urnScope, RawConfig: validConfig})
		assert.NoError(t, err)
	})
}

func TestSink(t *testing.T) {
	t.Run("should return error if frontier host returns error", func(t *testing.T) {
		ctx := context.TODO()

		client := new(mockClient)
		client.On("Connect", ctx, "frontier:80").Return(errors.New("failed to create connection"))
		frontierSink := frontier.New(client, testUtils.Logger)

		err := frontierSink.Init(ctx, plugins.Config{URNScope: urnScope, RawConfig: map[string]interface{}{
			"host": "frontier:80",
		}})
		require.Error(t, err)
		assert.EqualError(t, err, "error connecting to host: failed to create connection")
	})

	t.Run("should return RetryError if frontier returns certain status code", func(t *testing.T) {
		entity := models.NewEntity("", "user", "", "", map[string]interface{}{
			"email":     "user@raystack.com",
			"full_name": "john",
			"attributes": map[string]interface{}{
				"org_unit_path": "/",
			},
		})

		ctx := context.TODO()

		t.Run("when code is Unavailable", func(t *testing.T) {
			client := new(mockClient)
			client.On("Connect", ctx, "frontier:80").Return(nil)
			client.On("UpdateUser", ctx, mock.Anything, mock.Anything).Return(&frontierProto.UpdateUserResponse{}, status.Errorf(codes.Unavailable, ""))
			frontierSink := frontier.New(client, testUtils.Logger)
			err := frontierSink.Init(ctx, plugins.Config{RawConfig: map[string]interface{}{
				"host": validConfig["host"],
			}})
			assert.NoError(t, err)

			err = frontierSink.Sink(ctx, []models.Record{models.NewRecord(entity)})
			require.Error(t, err)
			assert.ErrorAs(t, err, &plugins.RetryError{})
		})

		t.Run("when error code is anything else", func(t *testing.T) {
			client := new(mockClient)
			client.On("Connect", ctx, "frontier:80").Return(nil)
			client.On("UpdateUser", ctx, mock.Anything, mock.Anything).Return(&frontierProto.UpdateUserResponse{}, status.Errorf(codes.Internal, ""))
			frontierSink := frontier.New(client, testUtils.Logger)
			err := frontierSink.Init(ctx, plugins.Config{RawConfig: map[string]interface{}{
				"host": validConfig["host"],
			}})

			assert.NoError(t, err)

			err = frontierSink.Sink(ctx, []models.Record{models.NewRecord(entity)})
			assert.ErrorContains(t, err, fmt.Sprintf("frontier returns code %d", codes.Internal))
		})

		t.Run("when not able to parse error", func(t *testing.T) {
			client := new(mockClient)
			client.On("Connect", ctx, "frontier:80").Return(nil)
			client.On("UpdateUser", ctx, mock.Anything, mock.Anything).Return(&frontierProto.UpdateUserResponse{}, fmt.Errorf("Some error"))
			frontierSink := frontier.New(client, testUtils.Logger)
			err := frontierSink.Init(ctx, plugins.Config{RawConfig: map[string]interface{}{
				"host": validConfig["host"],
			}})

			assert.NoError(t, err)

			err = frontierSink.Sink(ctx, []models.Record{models.NewRecord(entity)})
			assert.ErrorContains(t, err, "unable to parse error returned")
		})
	})

	t.Run("should not return when valid payload is sent", func(t *testing.T) {
		entity := models.NewEntity("", "user", "", "", map[string]interface{}{
			"full_name": "John Doe",
			"email":     "john.doe@raystack.com",
			"attributes": map[string]interface{}{
				"org_unit_path": "/",
				"aliases":       "doe.john@raystack.com,johndoe@raystack.com",
			},
		})

		ctx := context.TODO()

		client := new(mockClient)
		client.On("Connect", ctx, "frontier:80").Return(nil)
		client.On("UpdateUser", mock.AnythingOfType("*context.valueCtx"), mock.Anything, mock.Anything).Return(&frontierProto.UpdateUserResponse{}, nil)

		frontierSink := frontier.New(client, testUtils.Logger)
		err := frontierSink.Init(ctx, plugins.Config{RawConfig: map[string]interface{}{
			"host": validConfig["host"],
			"headers": map[string]interface{}{
				"X-Frontier-Email": "meteor@raystack.com",
				"X-Other-Header":   "value1, value2",
			},
		}})

		assert.NoError(t, err)

		err = frontierSink.Sink(ctx, []models.Record{models.NewRecord(entity)})
		assert.Equal(t, nil, err)
	})

	t.Run("should skip sink when error build user body", func(t *testing.T) {
		buildEntity := func(fullName, email string) *models.Record {
			props := map[string]interface{}{
				"full_name": fullName,
				"email":     email,
			}
			r := models.NewRecord(models.NewEntity("", "user", "", "", props))
			return &r
		}

		buildEntityWithAttrs := func(fullName, email string) *models.Record {
			props := map[string]interface{}{
				"full_name": fullName,
				"email":     email,
			}
			r := models.NewRecord(models.NewEntity("", "user", "", "", props))
			return &r
		}

		ctx := context.TODO()

		client := new(mockClient)
		client.On("Connect", ctx, "frontier:80").Return(nil)
		client.On("UpdateUser", ctx, mock.Anything, mock.Anything).Return(&frontierProto.UpdateUserResponse{}, nil)

		frontierSink := frontier.New(client, testUtils.Logger)
		err := frontierSink.Init(ctx, plugins.Config{RawConfig: map[string]interface{}{
			"host": validConfig["host"],
		}})
		assert.NoError(t, err)

		err = frontierSink.Sink(ctx, []models.Record{
			*buildEntity("", ""),
			*buildEntity("John Doe", ""),
			*buildEntityWithAttrs("John Doe", "john.doe@example.com"),
		})
		assert.Equal(t, nil, err)
	})
}

type mockClient struct {
	frontierProto.FrontierServiceClient
	mock.Mock
}

func (c *mockClient) Connect(ctx context.Context, host string) (err error) {
	args := c.Called(ctx, host)

	return args.Error(0)
}

func (c *mockClient) Close() error {
	args := c.Called()

	return args.Error(0)
}

func (c *mockClient) UpdateUser(ctx context.Context, in *frontierProto.UpdateUserRequest, opts ...grpc.CallOption) (*frontierProto.UpdateUserResponse, error) {
	args := c.Called(ctx, in, opts)

	return args.Get(0).(*frontierProto.UpdateUserResponse), args.Error(1)
}
