//go:build plugins
// +build plugins

package merlin

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/extractors/merlin/internal/merlin"
	intrnlmcks "github.com/goto/meteor/plugins/extractors/merlin/internal/mocks"
	"github.com/goto/meteor/test/mocks"
	testutils "github.com/goto/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	urnScope    = "test-merlin"
	credsBase64 = `eyJ0eXBlIjoic2VydmljZV9hY2NvdW50IiwicHJvamVjdF9pZCI6ImNvbXBhbnktZGF0YS1wbGF0Zm9ybSIsInByaXZhdGVfa2V5X2lkIjoiNjk4dnh2MzA4dzNpNjhwOTM4MDQwYno4MTdyOTViMWUwazRrbXZxcyIsInByaXZhdGVfa2V5IjoiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXG5NSUlFb1FJQkFBS0NBUUJWelEwV1B1YXFkd01OYXBDR0tkS1VSL01PZ1dOQnlydVQ2MFNKd2Q1bFkvMlNqeDF3XG5RNHNKNnhrLytUejdiVDNDZ05CQVBRK3JaZkxEMmZkUUpJQmVZRWxSY0h3NmEyUEEvNlRhWDJlNHFxMCs1eGszXG5nbkl0bHFabTBoUUVsWmQ3NkxObE1jSXRITm1uZUxJQ293VE9kemwwaFVkMklncnFMQjU0NXYzS09HZndvRUFwXG56M21QbS9pRjErelRQV3kwNDF3N2FqdldLMk4zbVJ5Z0tvUDc5bmUyZ0R1TjIrUUhtVzh3UEZ6UTNwZFFaVTY1XG4xbnBnUDlONHdSSFFUOHZvd1RVU1lkU1JaRzFwMU1QS0NYc3JxaE1Vdjd5TGRyT2NhY0F2Y1pxWU9jTWhKUTFwXG5iWXBzaW5EUjY1QVJEZHVNZUtvRVVrRmIzaGYyelBVY2RZTmhBZ01CQUFFQ2dnRUFRbGRPeENHVWxyOTRvN24rXG56MDJ0SGF2WUdpSWZEZkxrUUlZTHMzd3NLamM3REVRT0hneUxoL3E0eGtjL1NLUjV1VmVDTGZsSWtWMDliUU91XG5mdEFLVlc2Ym9oV1lhRTg2alRMZFUxK3JRaFR0NlpJa1pGQS9XbEoralVmbjVIZUo3bXZKc2ZmY1RLZGUvMmVLXG5OQkc2R0s0RXhieDd1Ykt1djh1bk1CSmlyeVV5Y2lvUHlrV1pFVllsNzIrMElCc0tDUU9YMzlGZC9wZ0pGOWpMXG5GUGVsZ0NzcnZQQS8zbG9kZ1F1M204VkVObHU0RzZ6M2tQUWdoQXZJMzd4QzlObFVOVnZ4MXl4Q3VrUWhmMHpRXG5RNTVrVVR3Z1o5c0lHR2NJLzJLNkgxWUh2K20zdm5NNUQ1aUw5ZVRIbjFIbmxHdHBsUUpobWhLakN4WElwYkh4XG5RVG9Pd1FLQmdRQ2NFWlA2SDNucTNlSDdkNXJvMWZ2QTZZRW9FUmZ6SXphVTRLazNTYjllMXRYallTejhjY052XG5LM2dac0hWMllaeTNxOW1DWW5jMG9Qd3d4NWRTd2h6cE9yQnJ3dnlvcFBia0twRDlXQ1h0WnRSa3dSVE43Q1hSXG5FKzJlU1NwdTJ5MTRTS3lzUFFvRFpteUpvOGJzN3JzZUxRVGlaZVVQbFlkbFA2YWRPR1NYK1FLQmdRQ012VnFFXG42bmJYNDFEY0xKdVV4VDAyNlQ5em5jbnBSdTNna2Z5WTBPNVFGOC9WY3E2eTVMeGRRdHlNTmJjYmtEWThpc0FNXG53VFA0S2FYUHVsMzhUT0NqZkczTU9ERGJ6bWVRMjdxS0wvOVVleWk4MTJCTjRYSXJwZ3VvUEtnRnRseWkxSk5IXG5aaVV0aW1lZE9vTkc0THV1REVxZU55VzFRbS9XbFF1NWZxS3dxUUtCZ0dzY3VWVzZFcCs2UnVXaXNlUEpNTzYyXG5rOWtlMmpRWjM5VVAxN05GWHgxRkR5anVRY1RFZzJBaUVseDNPamJVU1kzWldQL2VlbmZaWVJ4TmI3THgzSXZKXG5wdGxleXE4b0FQYVpyRWJrSDZ1dW5takVCM1pJODY5cUlQUTR2UEcyWlorZktUdFE3VFZtTDJuTHlMUkdLSkJPXG5UNExlY2ZaZkpyeTdrYXRuejhwcEFvR0FJMEZYeUkzM1lWTkhNVEJYZE9nSDBwYVJWNFFDVFZhQVJrNHJxWmhFXG42bmxjamNxaHFweVQ5d1RGdkxYRC9icWRhNE1TWXQrUEJpNWdvKzI2bDNZbW02MlN6NktQMHJBY3ozUExnY3hPXG5PTHAxVlFEYTFnZVFreENRUVArWTAzMkFMU1gxRXVDcWxZTGpPOGFwbGZxNzZQaVpSSkxwOWtNRFF3eXBHRGw1XG54YWtDZ1lBbTdwTzBMQS9oVHZkclo3ekdVSWZUVFp4ZjFxRCtXMGlVaDJNdHlhWk05dVFoRG9hYWhmN2YyVFQvXG50Mit3bHlJbEhNZFV4ZkRZZjhVNW93bDlJeXNxYVBNWnNRbVlOZ1ltWHBXOC9BaE5jS0Zuc2x5cnRkNTdPZjNDXG5sRkhwTndmak5seERUc3FsMmtXYmN3SmJZMEVibFBSSXRwbEU3Z0RsVXZmZ1NOVGorZz09XG4tLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tXG4iLCJjbGllbnRfZW1haWwiOiJzeXN0ZW1zLW1ldGVvckBjb21wYW55LWRhdGEtcGxhdGZvcm0uaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLCJjbGllbnRfaWQiOiIwNDMxNjE2ODg4ODA0MzA3OTU4OTMiLCJhdXRoX3VyaSI6Imh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbS9vL29hdXRoMi9hdXRoIiwidG9rZW5fdXJpIjoiaHR0cHM6Ly9vYXV0aDIuZ29vZ2xlYXBpcy5jb20vdG9rZW4iLCJhdXRoX3Byb3ZpZGVyX3g1MDlfY2VydF91cmwiOiJodHRwczovL3d3dy5nb29nbGVhcGlzLmNvbS9vYXV0aDIvdjEvY2VydHMiLCJjbGllbnRfeDUwOV9jZXJ0X3VybCI6Imh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL3JvYm90L3YxL21ldGFkYXRhL3g1MDkvc3lzdGVtcy1tZXRlb3IlNDBjb21wYW55LWRhdGEtcGxhdGZvcm0uaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20ifQ==`
	hostURL     = "merlin.com/api/merlin"
)

var ctx = context.Background()

func TestInit(t *testing.T) {
	t.Run("should return error if config is invalid", func(t *testing.T) {
		extr := New(testutils.Logger, func(ctx context.Context, cfg Config) (Client, error) {
			return nil, errors.New("unexpected call")
		})
		err := extr.Init(ctx, plugins.Config{
			URNScope:  urnScope,
			RawConfig: map[string]interface{}{},
		})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should try to create a new client if config is valid", func(t *testing.T) {
		extr := New(testutils.Logger, func(ctx context.Context, cfg Config) (Client, error) {
			expected := Config{
				URL:                  hostURL,
				ServiceAccountBase64: credsBase64,
				RequestTimeout:       30 * time.Second,
				WorkerCount:          10,
			}
			assert.Equal(t, expected, cfg)
			return intrnlmcks.NewMerlinClient(t), nil
		})
		err := extr.Init(ctx, plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"url":                    hostURL,
				"service_account_base64": credsBase64,
				"request_timeout":        "30s",
				"worker_count":           10,
			},
		})
		assert.NoError(t, err)
	})

	t.Run("should return error if credentials is not a base64 string", func(t *testing.T) {
		extr := New(testutils.Logger, newHTTPClient)
		err := extr.Init(ctx, plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"url":                    hostURL,
				"service_account_base64": "Good Times Bad Times",
			},
		})
		assert.Error(t, err)
	})
}

func TestExtract(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		isRetryErr bool
	}{
		{
			name:       "ObtuseError",
			err:        errors.New("Mambo No. 5"),
			isRetryErr: false,
		},
		{
			name:       "ContextDeadlineExceeded",
			err:        fmt.Errorf("wrapped for dramatic effect: %w", context.DeadlineExceeded),
			isRetryErr: true,
		},
		{
			name:       "5xx",
			err:        fmt.Errorf("egg wrap: %w", &merlin.APIError{Status: 503}),
			isRetryErr: true,
		},
		{
			name:       "429",
			err:        fmt.Errorf("salad wrap: %w", &merlin.APIError{Status: 429}),
			isRetryErr: true,
		},
	}
	for _, tc := range cases {
		t.Run("ProjectsCallFailure/"+tc.name, func(t *testing.T) {
			m := intrnlmcks.NewMerlinClient(t)
			m.EXPECT().Projects(testutils.OfTypeContext()).
				Return(nil, tc.err)
			extr := initialisedExtr(t, m)

			err := extr.Extract(ctx, mocks.NewEmitter().Push)
			assert.Error(t, err)
			assert.Equal(t, tc.isRetryErr, errors.As(err, &plugins.RetryError{}))
		})
	}

	t.Run("it should tolerate models, version fetch failures", func(t *testing.T) {
		mc := intrnlmcks.NewMerlinClient(t)

		var projects []merlin.Project
		testutils.LoadJSON(t, "testdata/mocked-projects.json", &projects)
		mc.EXPECT().Projects(testutils.OfTypeContext()).
			Return(projects, nil)

		var models []merlin.Model
		testutils.LoadJSON(t, "testdata/mocked-models-1.json", &models)
		mc.EXPECT().Models(testutils.OfTypeContext(), int64(1)).Return(models, nil)
		mc.EXPECT().ModelVersion(testutils.OfTypeContext(), int64(80), int64(2)).
			Return(merlin.ModelVersion{}, errors.New("I Want It All"))
		mc.EXPECT().ModelVersion(testutils.OfTypeContext(), int64(689), int64(7)).
			Return(merlin.ModelVersion{}, errors.New("Knights In White Satin"))

		mc.EXPECT().Models(testutils.OfTypeContext(), int64(100)).
			Return(nil, errors.New("Losing My Edge"))
		mc.EXPECT().Models(testutils.OfTypeContext(), int64(200)).
			Return(nil, errors.New("Paranoid"))

		emitter := mocks.NewEmitter()
		err := initialisedExtr(t, mc).Extract(ctx, emitter.Push)
		assert.NoError(t, err)

		actual := emitter.GetAllData()
		assert.Empty(t, actual)
	})

	t.Run("should build models from Merlin", func(t *testing.T) {
		mc := intrnlmcks.NewMerlinClient(t)

		var projects []merlin.Project
		testutils.LoadJSON(t, "testdata/mocked-projects.json", &projects)
		mc.EXPECT().Projects(testutils.OfTypeContext()).
			Return(projects, nil)

		var models []merlin.Model
		testutils.LoadJSON(t, "testdata/mocked-models-1.json", &models)
		mc.EXPECT().Models(testutils.OfTypeContext(), int64(1)).Return(models, nil)

		var mv merlin.ModelVersion
		testutils.LoadJSON(t, "testdata/mocked-model-version-80-2.json", &mv)
		mc.EXPECT().ModelVersion(testutils.OfTypeContext(), int64(80), int64(2)).
			Return(mv, nil)

		mv = merlin.ModelVersion{}
		testutils.LoadJSON(t, "testdata/mocked-model-version-689-7.json", &mv)
		mc.EXPECT().ModelVersion(testutils.OfTypeContext(), int64(689), int64(7)).
			Return(mv, nil)

		models = nil
		testutils.LoadJSON(t, "testdata/mocked-models-100.json", &models)
		mc.EXPECT().Models(testutils.OfTypeContext(), int64(100)).Return(models, nil)

		mv = merlin.ModelVersion{}
		testutils.LoadJSON(t, "testdata/mocked-model-version-1376-47.json", &mv)
		mc.EXPECT().ModelVersion(testutils.OfTypeContext(), int64(1376), int64(47)).
			Return(mv, nil)

		mv = merlin.ModelVersion{}
		testutils.LoadJSON(t, "testdata/mocked-model-version-284-582.json", &mv)
		mc.EXPECT().ModelVersion(testutils.OfTypeContext(), int64(284), int64(582)).
			Return(mv, nil)

		mc.EXPECT().Models(testutils.OfTypeContext(), int64(200)).Return(nil, nil)

		emitter := mocks.NewEmitter()
		err := initialisedExtr(t, mc).Extract(ctx, emitter.Push)
		assert.NoError(t, err)

		actual := emitter.GetAllData()
		testutils.AssertProtosWithJSONFile(t, "testdata/expected-assets.json", actual)
	})

	t.Run("it should recover from panics in workers", func(t *testing.T) {
		mc := intrnlmcks.NewMerlinClient(t)

		var projects []merlin.Project
		testutils.LoadJSON(t, "testdata/mocked-projects.json", &projects)
		mc.EXPECT().Projects(testutils.OfTypeContext()).
			Return(projects, nil)

		panicErr := errors.New("Starman")
		mc.EXPECT().Models(testutils.OfTypeContext(), int64(1)).
			Run(func(ctx context.Context, projectID int64) {
				panic(panicErr)
			})

		err := initialisedExtr(t, mc).Extract(ctx, mocks.NewEmitter().Push)
		assert.ErrorIs(t, err, panicErr)
	})
}

func initialisedExtr(t *testing.T, m *intrnlmcks.MerlinClient) *Extractor {
	extr := New(testutils.Logger, func(context.Context, Config) (Client, error) {
		return m, nil
	})
	require.NoError(t, extr.Init(ctx, plugins.Config{
		URNScope: urnScope,
		RawConfig: map[string]interface{}{
			"url":          hostURL,
			"worker_count": 1,
		},
	}))
	return extr
}
