//go:build plugins
// +build plugins

package shield_test

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/odpf/meteor/models"
	testUtils "github.com/odpf/meteor/test/utils"
	"github.com/odpf/meteor/utils"

	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/plugins"
	shield "github.com/odpf/meteor/plugins/sinks/shield"
	shieldProto "github.com/odpf/shield/proto/v1beta1"
	"github.com/stretchr/testify/assert"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	validConfig = map[string]interface{}{
		"host": "shield:80",
	}
	urnScope = "test-shield"
)

func TestInit(t *testing.T) {
	t.Run("should return error if config is invalid", func(t *testing.T) {
		sink := shield.New(new(mockClient), testUtils.Logger)
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

		sink := shield.New(client, testUtils.Logger)
		err = sink.Init(ctx, plugins.Config{URNScope: urnScope, RawConfig: validConfig})
		assert.NoError(t, err)
	})
}

func TestSink(t *testing.T) {
	t.Run("should return RetryError if shield returns certain status code", func(t *testing.T) {

		user, err := anypb.New(&v1beta2.User{
			Email:    "user@odpf.com",
			FullName: "john",
			Attributes: utils.TryParseMapToProto(map[string]interface{}{
				"org_unit_path": "/",
			}),
		})
		require.NoError(t, err)

		data := &v1beta2.Asset{
			Data: user,
		}

		ctx := context.TODO()

		client := new(mockClient)
		client.On("Connect", ctx, "shield:80").Return(nil)
		client.On("UpdateUser", ctx, mock.Anything, mock.Anything).Return(&shieldProto.UpdateUserResponse{}, status.Errorf(codes.Unavailable, ""))
		shieldSink := shield.New(client, testUtils.Logger)
		err = shieldSink.Init(ctx, plugins.Config{RawConfig: map[string]interface{}{
			"host": validConfig["host"],
		}})
		if err != nil {
			t.Fatal(err)
		}

		err = shieldSink.Sink(ctx, []models.Record{models.NewRecord(data)})
		require.Error(t, err)
		assert.True(t, errors.Is(err, plugins.RetryError{}))

	})

	t.Run("should not return when valid payload is sent", func(t *testing.T) {
		u := &v1beta2.User{
			FullName: "John Doe",
			Email:    "john.doe@odpf.com",
			Attributes: utils.TryParseMapToProto(map[string]interface{}{
				"org_unit_path": "/",
				"aliases":       "doe.john@odpf.com,johndoe@odpf.com",
			}),
		}
		user, _ := anypb.New(u)
		data := &v1beta2.Asset{
			Data: user,
		}

		ctx := context.TODO()

		client := new(mockClient)
		client.On("Connect", ctx, "shield:80").Return(nil)
		client.On("UpdateUser", ctx, mock.Anything, mock.Anything).Return(&shieldProto.UpdateUserResponse{}, nil)

		shieldSink := shield.New(client, testUtils.Logger)
		err := shieldSink.Init(ctx, plugins.Config{RawConfig: map[string]interface{}{
			"host": validConfig["host"],
		}})
		if err != nil {
			t.Fatal(err)
		}

		err = shieldSink.Sink(ctx, []models.Record{models.NewRecord(data)})
		assert.Equal(t, nil, err)
	})

}

type mockClient struct {
	shieldProto.ShieldServiceClient
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

func (c *mockClient) UpdateUser(ctx context.Context, in *shieldProto.UpdateUserRequest, opts ...grpc.CallOption) (*shieldProto.UpdateUserResponse, error) {
	args := c.Called(ctx, in, opts)

	return args.Get(0).(*shieldProto.UpdateUserResponse), args.Error(1)
}
