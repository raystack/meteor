//go:build plugins
// +build plugins

package compass_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/sinks/compass"
	testutils "github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

var host = "http://compass.com"

func TestInit(t *testing.T) {
	t.Run("should return InvalidConfigError on invalid config", func(t *testing.T) {
		invalidConfigs := []map[string]any{
			{
				"host": "",
			},
		}
		for i, config := range invalidConfigs {
			t.Run(fmt.Sprintf("test invalid config #%d", i+1), func(t *testing.T) {
				client := &mockHTTPClient{}
				compassSink := compass.New(client, testutils.Logger)
				err := compassSink.Init(context.TODO(), plugins.Config{RawConfig: config})

				assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
			})
		}
	})
}

func TestSink(t *testing.T) {
	upsertEntityURL := fmt.Sprintf("%s/raystack.compass.v1beta1.CompassService/UpsertEntity", host)
	upsertEdgeURL := fmt.Sprintf("%s/raystack.compass.v1beta1.CompassService/UpsertEdge", host)

	t.Run("should return error if compass host returns error", func(t *testing.T) {
		client := &mockHTTPClient{}
		client.SetupResponse(404, `{"reason":"not found"}`)
		ctx := context.TODO()

		compassSink := compass.New(client, testutils.Logger)
		err := compassSink.Init(ctx, plugins.Config{RawConfig: map[string]any{
			"host": host,
		}})
		require.NoError(t, err)

		entity := &meteorv1beta1.Entity{Type: "table"}
		err = compassSink.Sink(ctx, []models.Record{models.NewRecord(entity)})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "compass returns 404")
	})

	t.Run("should return RetryError if compass returns 5xx status code", func(t *testing.T) {
		for _, code := range []int{500, 501, 502, 503, 504, 505} {
			t.Run(fmt.Sprintf("%d status code", code), func(t *testing.T) {
				client := &mockHTTPClient{}
				client.SetupResponse(code, `{"reason":"internal server error"}`)
				ctx := context.TODO()

				compassSink := compass.New(client, testutils.Logger)
				err := compassSink.Init(ctx, plugins.Config{RawConfig: map[string]any{
					"host": host,
				}})
				require.NoError(t, err)

				entity := &meteorv1beta1.Entity{Type: "table"}
				err = compassSink.Sink(ctx, []models.Record{models.NewRecord(entity)})
				require.Error(t, err)
				assert.ErrorAs(t, err, &plugins.RetryError{})
			})
		}
	})

	t.Run("should send correct UpsertEntity request", func(t *testing.T) {
		client := &mockHTTPClient{}
		client.SetupResponse(200, `{"id":"uuid-1"}`)
		ctx := context.TODO()

		compassSink := compass.New(client, testutils.Logger)
		err := compassSink.Init(ctx, plugins.Config{RawConfig: map[string]any{
			"host": host,
		}})
		require.NoError(t, err)

		props, err := structpb.NewStruct(map[string]any{
			"url": "http://test.com",
			"labels": map[string]any{
				"labelA": "valueLabelA",
				"labelB": "valueLabelB",
			},
			"columns": []any{
				map[string]any{
					"name":        "id",
					"description": "It is the ID",
					"data_type":   "INT",
					"is_nullable": true,
				},
			},
		})
		require.NoError(t, err)
		entity := &meteorv1beta1.Entity{
			Urn:         "my-topic-urn",
			Name:        "my-topic",
			Source:      "kafka",
			Type:        "topic",
			Description: "topic information",
			Properties:  props,
		}
		err = compassSink.Sink(ctx, []models.Record{models.NewRecord(entity)})
		assert.NoError(t, err)

		// Should have exactly 1 request (UpsertEntity, no owners).
		require.Len(t, client.requests, 1)
		req := client.requests[0]
		assert.Equal(t, http.MethodPost, req.Method)
		assert.Equal(t, upsertEntityURL, reqURL(req))

		var entityReq compass.UpsertEntityRequest
		decodeBody(t, req, &entityReq)
		assert.Equal(t, "my-topic-urn", entityReq.URN)
		assert.Equal(t, "topic", entityReq.Type)
		assert.Equal(t, "my-topic", entityReq.Name)
		assert.Equal(t, "topic information", entityReq.Description)
		assert.Equal(t, "kafka", entityReq.Source)
		assert.Equal(t, "http://test.com", entityReq.Properties["url"])
		assert.Equal(t, map[string]any{"labelA": "valueLabelA", "labelB": "valueLabelB"}, entityReq.Properties["labels"])
		// Data fields are flattened into properties.
		assert.NotNil(t, entityReq.Properties["columns"])
	})

	t.Run("should send upstreams and downstreams in entity request", func(t *testing.T) {
		client := &mockHTTPClient{}
		client.SetupResponse(200, `{}`)
		ctx := context.TODO()

		compassSink := compass.New(client, testutils.Logger)
		err := compassSink.Init(ctx, plugins.Config{RawConfig: map[string]any{
			"host": host,
		}})
		require.NoError(t, err)

		entity := &meteorv1beta1.Entity{
			Urn:    "my-topic-urn",
			Name:   "my-topic",
			Source: "kafka",
			Type:   "topic",
		}
		edges := []*meteorv1beta1.Edge{
			{SourceUrn: "urn-1", TargetUrn: "my-topic-urn", Type: "lineage"},
			{SourceUrn: "urn-2", TargetUrn: "my-topic-urn", Type: "lineage"},
			{SourceUrn: "my-topic-urn", TargetUrn: "urn-3", Type: "lineage"},
		}
		err = compassSink.Sink(ctx, []models.Record{models.NewRecord(entity, edges...)})
		assert.NoError(t, err)

		require.Len(t, client.requests, 1)
		var entityReq compass.UpsertEntityRequest
		decodeBody(t, client.requests[0], &entityReq)
		assert.Equal(t, []string{"urn-1", "urn-2"}, entityReq.Upstreams)
		assert.Equal(t, []string{"urn-3"}, entityReq.Downstreams)
	})

	t.Run("should send ownership edges", func(t *testing.T) {
		client := &mockHTTPClient{}
		client.SetupResponse(200, `{}`)
		ctx := context.TODO()

		compassSink := compass.New(client, testutils.Logger)
		err := compassSink.Init(ctx, plugins.Config{RawConfig: map[string]any{
			"host": host,
		}})
		require.NoError(t, err)

		entity := &meteorv1beta1.Entity{
			Urn:    "my-topic-urn",
			Name:   "my-topic",
			Source: "kafka",
			Type:   "topic",
		}
		edges := []*meteorv1beta1.Edge{
			{SourceUrn: "my-topic-urn", TargetUrn: "urn:user:alice@company.com", Type: "owned_by", Source: "meteor"},
			{SourceUrn: "my-topic-urn", TargetUrn: "urn:user:bob@company.com", Type: "owned_by", Source: "meteor"},
		}
		err = compassSink.Sink(ctx, []models.Record{models.NewRecord(entity, edges...)})
		assert.NoError(t, err)

		// 1 entity + 2 edge requests.
		require.Len(t, client.requests, 3)

		// First request is entity upsert.
		assert.Equal(t, upsertEntityURL, reqURL(client.requests[0]))

		// Second and third are edge upserts.
		var edge1 compass.UpsertEdgeRequest
		decodeBody(t, client.requests[1], &edge1)
		assert.Equal(t, upsertEdgeURL, reqURL(client.requests[1]))
		assert.Equal(t, "my-topic-urn", edge1.SourceURN)
		assert.Equal(t, "urn:user:alice@company.com", edge1.TargetURN)
		assert.Equal(t, "owned_by", edge1.Type)
		assert.Equal(t, "meteor", edge1.Source)

		var edge2 compass.UpsertEdgeRequest
		decodeBody(t, client.requests[2], &edge2)
		assert.Equal(t, "urn:user:bob@company.com", edge2.TargetURN)
	})

	t.Run("should send headers from config", func(t *testing.T) {
		client := &mockHTTPClient{}
		client.SetupResponse(200, `{}`)
		ctx := context.TODO()

		compassSink := compass.New(client, testutils.Logger)
		err := compassSink.Init(ctx, plugins.Config{RawConfig: map[string]any{
			"host": host,
			"headers": map[string]string{
				"Compass-User-UUID": "meteor@raystack.io",
				"X-Custom":          "val1, val2",
			},
		}})
		require.NoError(t, err)

		entity := &meteorv1beta1.Entity{
			Urn:    "my-urn",
			Name:   "my-name",
			Source: "kafka",
			Type:   "topic",
		}
		err = compassSink.Sink(ctx, []models.Record{models.NewRecord(entity)})
		assert.NoError(t, err)

		require.Len(t, client.requests, 1)
		req := client.requests[0]
		assert.Contains(t, req.Header.Get("Compass-User-UUID"), "meteor@raystack.io")
	})

	t.Run("should flatten data into properties without @type", func(t *testing.T) {
		client := &mockHTTPClient{}
		client.SetupResponse(200, `{}`)
		ctx := context.TODO()

		compassSink := compass.New(client, testutils.Logger)
		err := compassSink.Init(ctx, plugins.Config{RawConfig: map[string]any{
			"host": host,
		}})
		require.NoError(t, err)

		props, err := structpb.NewStruct(map[string]any{
			"attributes": map[string]any{
				"attrA": "valueA",
			},
		})
		require.NoError(t, err)
		entity := &meteorv1beta1.Entity{
			Urn:        "my-topic-urn",
			Name:       "my-topic",
			Source:     "kafka",
			Type:       "topic",
			Properties: props,
		}
		err = compassSink.Sink(ctx, []models.Record{models.NewRecord(entity)})
		assert.NoError(t, err)

		require.Len(t, client.requests, 1)
		var reqEntity compass.UpsertEntityRequest
		decodeBody(t, client.requests[0], &reqEntity)
		// @type should not be in properties (no longer relevant with Entity model).
		_, hasType := reqEntity.Properties["@type"]
		assert.False(t, hasType)
		// Attributes should be present in properties.
		assert.NotNil(t, reqEntity.Properties["attributes"])
	})
}

// mockHTTPClient records all requests and returns a fixed response.
type mockHTTPClient struct {
	ResponseStatus int
	ResponseJSON   string
	requests       []*http.Request
	bodies         [][]byte
}

func (m *mockHTTPClient) SetupResponse(statusCode int, jsonResp string) {
	m.ResponseStatus = statusCode
	m.ResponseJSON = jsonResp
}

func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	// Read and store the body so tests can inspect it.
	var bodyBytes []byte
	if req.Body != nil {
		var err error
		bodyBytes, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	}
	m.requests = append(m.requests, req)
	m.bodies = append(m.bodies, bodyBytes)

	return &http.Response{
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		StatusCode:    m.ResponseStatus,
		Request:       req,
		Header:        make(http.Header),
		ContentLength: int64(len(m.ResponseJSON)),
		Body:          io.NopCloser(bytes.NewBufferString(m.ResponseJSON)),
	}, nil
}

func reqURL(req *http.Request) string {
	return fmt.Sprintf("%s://%s%s", req.URL.Scheme, req.URL.Host, req.URL.Path)
}

func decodeBody(t *testing.T, req *http.Request, v any) {
	t.Helper()
	// Find the index of this request in the mock to get the stored body.
	bodyBytes, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(bodyBytes, v))
}
