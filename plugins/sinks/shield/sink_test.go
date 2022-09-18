package shield_test

import (
	"context"
	"fmt"
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
	sh "github.com/odpf/shield/proto/v1beta1"
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
		invalidConfigs := []map[string]interface{}{
			{
				"host": "",
			},
		}
		for i, config := range invalidConfigs {
			t.Run(fmt.Sprintf("test invalid config #%d", i+1), func(t *testing.T) {
				sink := shield.New(new(mockClient), testUtils.Logger)
				err := sink.Init(context.TODO(), plugins.Config{RawConfig: config})
				assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
			})
		}
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
	t.Run("should return error if shield host returns error", func(t *testing.T) {
		ctx := context.TODO()

		client := new(mockClient)
		client.On("Connect", ctx, "shield:80").Return(errors.New("failed to create connection"))
		shieldSink := shield.New(client, testUtils.Logger)

		err := shieldSink.Init(ctx, plugins.Config{URNScope: urnScope, RawConfig: map[string]interface{}{
			"host": "shield:80",
		}})
		require.Error(t, err)
		assert.EqualError(t, err, "error connecting to host: failed to create connection")
	})

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
		client.On("UpdateUser", ctx, mock.Anything, mock.Anything).Return(&sh.UpdateUserResponse{}, status.Errorf(codes.Unavailable, ""))
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

	t.Run("should return error when invalid payload is sent", func(t *testing.T) {
		testData := []struct {
			User    *v1beta2.User
			wantErr error
		}{
			{
				User: &v1beta2.User{
					FullName:   "",
					Email:      "",
					Attributes: utils.TryParseMapToProto(map[string]interface{}{}),
				},
				wantErr: errors.Wrap(errors.New(fmt.Sprintf("unexpected type %T for name, must be a string", nil)), "failed to build shield payload"),
			},
			{
				User: &v1beta2.User{
					FullName:   "John Doe",
					Email:      "",
					Attributes: utils.TryParseMapToProto(map[string]interface{}{}),
				},
				wantErr: errors.Wrap(errors.New(fmt.Sprintf("unexpected type %T for email, must be a string", nil)), "failed to build shield payload"),
			},
			{
				User: &v1beta2.User{
					FullName: "John Doe",
					Email:    "john.doe@odpf.com",
				},
				wantErr: errors.Wrap(errors.New(fmt.Sprintf("unexpected type %T for attributes, must be a map[string]interface{}", nil)), "failed to build shield payload"),
			},
		}

		for _, d := range testData {
			user, _ := anypb.New(d.User)
			data := &v1beta2.Asset{
				Data: user,
			}
			ctx := context.TODO()

			client := new(mockClient)
			client.On("Connect", ctx, "shield:80").Return(nil)
			shieldSink := shield.New(client, testUtils.Logger)
			err := shieldSink.Init(ctx, plugins.Config{RawConfig: map[string]interface{}{
				"host": validConfig["host"],
			}})
			if err != nil {
				t.Fatal(err)
			}

			err = shieldSink.Sink(ctx, []models.Record{models.NewRecord(data)})
			require.Error(t, err)
			assert.Equal(t, d.wantErr.Error(), err.Error())
		}
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
		client.On("UpdateUser", ctx, mock.Anything, mock.Anything).Return(&sh.UpdateUserResponse{}, nil)

		shieldSink := shield.New(client, testUtils.Logger)

		shieldSink = shield.New(client, testUtils.Logger)
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
	sh.ShieldServiceClient
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

func (c *mockClient) UpdateUser(ctx context.Context, in *sh.UpdateUserRequest, opts ...grpc.CallOption) (*sh.UpdateUserResponse, error) {
	args := c.Called(ctx, in, opts)

	return args.Get(0).(*sh.UpdateUserResponse), args.Error(1)
}
