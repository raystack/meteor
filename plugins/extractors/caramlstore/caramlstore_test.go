//go:build plugins
// +build plugins

package caramlstore

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/extractors/caramlstore/internal/core"
	internalmocks "github.com/goto/meteor/plugins/extractors/caramlstore/internal/mocks"
	"github.com/goto/meteor/test/mocks"
	testutils "github.com/goto/meteor/test/utils"
	"github.com/goto/meteor/utils"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const (
	bufSize  = 1024 * 1024
	urnScope = "test-caramlstore"
)

var ctx = context.Background()

func TestInit(t *testing.T) {
	t.Run("should return error if config is invalid", func(t *testing.T) {
		extr := New(testutils.Logger, newGRPCClient())
		err := extr.Init(context.Background(), plugins.Config{
			URNScope:  urnScope,
			RawConfig: map[string]interface{}{},
		})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should connect to caraml-store to check connection if config is valid", func(t *testing.T) {
		hostURL := "caraml-store:80"
		client := internalmocks.NewCaraMLClient(t)
		client.EXPECT().Connect(testutils.OfTypeContext(), hostURL, 100, 30*time.Second).
			Return(nil)

		extr := New(testutils.Logger, client)
		err := extr.Init(ctx, plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"url":             hostURL,
				"max_size_in_mb":  100,
				"request_timeout": "30s",
			},
		})
		assert.NoError(t, err)
	})

	t.Run("should return error if connect fails", func(t *testing.T) {
		hostURL := "caraml-store:80"
		client := internalmocks.NewCaraMLClient(t)
		client.EXPECT().Connect(testutils.OfTypeContext(), hostURL, 100, 30*time.Second).
			Return(errors.New("25 or 6 to 4"))

		extr := New(testutils.Logger, client)
		err := extr.Init(ctx, plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"url":             hostURL,
				"max_size_in_mb":  100,
				"request_timeout": "30s",
			},
		})
		assert.Error(t, err)
	})
}

func TestExtract(t *testing.T) {
	cases := []struct {
		err        error
		expected   codes.Code
		isRetryErr bool
	}{
		{
			err:        status.Error(codes.NotFound, "SBI employees"),
			expected:   codes.NotFound,
			isRetryErr: false,
		},
		{
			err:        errors.New("By the power of Grayskull.."),
			expected:   codes.Unknown,
			isRetryErr: false,
		},
		{
			err:        status.Error(codes.Canceled, "Right Place Wrong Time"),
			expected:   codes.Canceled,
			isRetryErr: true,
		},
		{
			err:        status.Error(codes.DeadlineExceeded, "Welcome to the machine"),
			expected:   codes.DeadlineExceeded,
			isRetryErr: true,
		},
		{
			err:        status.Error(codes.ResourceExhausted, "White room"),
			expected:   codes.ResourceExhausted,
			isRetryErr: true,
		},
		{
			err:        status.Error(codes.Internal, "Everything in its right place"),
			expected:   codes.Internal,
			isRetryErr: true,
		},
		{
			err:        status.Error(codes.Unavailable, "Sharp Dressed Man"),
			expected:   codes.Unavailable,
			isRetryErr: true,
		},
	}
	for _, tc := range cases {
		name := "ListProjectFailure/Code=" + utils.StatusCode(tc.err).String()
		t.Run(name, func(t *testing.T) {
			m, lis := newMockServer(t)
			m.EXPECT().ListProjects(testutils.OfTypeContext(), &core.ListProjectsRequest{}).
				Return(nil, tc.err)

			extr := New(testutils.Logger, newBufGRPCClient(t, lis))
			require.NoError(t, extr.Init(ctx, plugins.Config{
				URNScope: urnScope,
				RawConfig: map[string]interface{}{
					"url": lis.Addr().String(),
				},
			}))

			err := extr.Extract(ctx, mocks.NewEmitter().Push)
			assert.Error(t, err)
			assert.Equal(t, tc.expected, utils.StatusCode(err))
			assert.Equal(t, tc.isRetryErr, errors.As(err, &plugins.RetryError{}))
		})
	}

	t.Run("it should tolerate entity, feature table fetch failures", func(t *testing.T) {
		m, lis := newMockServer(t)
		m.EXPECT().ListProjects(testutils.OfTypeContext(), &core.ListProjectsRequest{}).
			Return(&core.ListProjectsResponse{Projects: []string{"odokawa", "gouriki", "dobu"}}, nil)

		// Failed to fetch entities, should not attempt to fetch feature tables
		m.EXPECT().ListEntities(testutils.OfTypeContext(), &core.ListEntitiesRequest{
			Filter: &core.ListEntitiesRequest_Filter{Project: "odokawa"},
		}).Return(nil, status.Error(codes.Internal, "odd taxi"))

		// Fetched entities but failed to fetch table features
		m.EXPECT().ListEntities(testutils.OfTypeContext(), &core.ListEntitiesRequest{
			Filter: &core.ListEntitiesRequest_Filter{Project: "gouriki"},
		}).Return(&core.ListEntitiesResponse{}, nil)
		m.EXPECT().ListFeatureTables(testutils.OfTypeContext(), &core.ListFeatureTablesRequest{
			Filter: &core.ListFeatureTablesRequest_Filter{Project: "gouriki"},
		}).Return(nil, status.Error(codes.Internal, "nani!"))

		// Fetched entities but not the ones that are in the feature tables,
		// should drop records and continue
		m.EXPECT().ListEntities(testutils.OfTypeContext(), &core.ListEntitiesRequest{
			Filter: &core.ListEntitiesRequest_Filter{Project: "dobu"},
		}).Return(&core.ListEntitiesResponse{}, nil)
		var featureTablesResp core.ListFeatureTablesResponse
		testutils.LoadJSON(t, "testdata/mocked-feature-tables-sauron.json", &featureTablesResp)
		m.EXPECT().ListFeatureTables(testutils.OfTypeContext(), &core.ListFeatureTablesRequest{
			Filter: &core.ListFeatureTablesRequest_Filter{Project: "dobu"},
		}).Return(&featureTablesResp, nil)

		extr := New(testutils.Logger, newBufGRPCClient(t, lis))
		require.NoError(t, extr.Init(ctx, plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"url": lis.Addr().String(),
			},
		}))

		emitter := mocks.NewEmitter()
		err := extr.Extract(ctx, emitter.Push)
		assert.NoError(t, err)

		actual := emitter.GetAllData()
		assert.Empty(t, actual)
	})

	t.Run("should build feature tables from caraml-store", func(t *testing.T) {
		mockServer, lis := newMockServer(t)

		mockServer.EXPECT().ListProjects(testutils.OfTypeContext(), &core.ListProjectsRequest{}).
			Return(
				&core.ListProjectsResponse{Projects: []string{"sauron", "food-tensoba"}}, nil,
			)

		var entitiesResp core.ListEntitiesResponse
		testutils.LoadJSON(t, "testdata/mocked-entities-sauron.json", &entitiesResp)
		mockServer.EXPECT().ListEntities(testutils.OfTypeContext(), &core.ListEntitiesRequest{
			Filter: &core.ListEntitiesRequest_Filter{Project: "sauron"},
		}).Return(&entitiesResp, nil)

		var featureTablesResp core.ListFeatureTablesResponse
		testutils.LoadJSON(t, "testdata/mocked-feature-tables-sauron.json", &featureTablesResp)
		mockServer.EXPECT().ListFeatureTables(testutils.OfTypeContext(), &core.ListFeatureTablesRequest{
			Filter: &core.ListFeatureTablesRequest_Filter{Project: "sauron"},
		}).Return(&featureTablesResp, nil)

		testutils.LoadJSON(t, "testdata/mocked-entities-food-tensoba.json", &entitiesResp)
		mockServer.EXPECT().ListEntities(testutils.OfTypeContext(), &core.ListEntitiesRequest{
			Filter: &core.ListEntitiesRequest_Filter{Project: "food-tensoba"},
		}).Return(&entitiesResp, nil)

		testutils.LoadJSON(t, "testdata/mocked-feature-tables-food-tensoba.json", &featureTablesResp)
		mockServer.EXPECT().ListFeatureTables(testutils.OfTypeContext(), &core.ListFeatureTablesRequest{
			Filter: &core.ListFeatureTablesRequest_Filter{Project: "food-tensoba"},
		}).Return(&featureTablesResp, nil)

		extr := New(testutils.Logger, newBufGRPCClient(t, lis))
		require.NoError(t, extr.Init(ctx, plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"url": lis.Addr().String(),
			},
		}))

		emitter := mocks.NewEmitter()
		err := extr.Extract(ctx, emitter.Push)
		assert.NoError(t, err)

		actual := emitter.GetAllData()
		testutils.AssertProtosWithJSONFile(t, "testdata/expected-assets.json", actual)
	})
}

func newMockServer(t *testing.T) (*internalmocks.CoreServer, *bufconn.Listener) {
	t.Helper()

	s := grpc.NewServer()
	mockServer := internalmocks.NewCoreServer(t)
	core.RegisterCoreServiceServer(s, mockServer)

	lis := bufconn.Listen(bufSize)
	go func() {
		if err := s.Serve(lis); err != nil {
			t.Error("Serve mock caraml store core server", err)
		}
	}()

	t.Cleanup(func() {
		s.Stop()
		_ = lis.Close()
	})

	return mockServer, lis
}

func newBufGRPCClient(t *testing.T, lis *bufconn.Listener) *gRPCClient {
	t.Helper()

	return newGRPCClient(grpc.WithContextDialer(
		func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		},
	))
}
