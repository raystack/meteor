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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/sinks/compass"
	testUtils "github.com/goto/meteor/test/utils"
	"github.com/goto/meteor/utils"
	"github.com/stretchr/testify/assert"
)

var (
	host = "http://compass.com"
)

// sample metadata
var (
	url = fmt.Sprintf("%s/v1beta1/assets", host)
)

func TestInit(t *testing.T) {
	t.Run("should return InvalidConfigError on invalid config", func(t *testing.T) {
		invalidConfigs := []map[string]interface{}{
			{
				"host": "",
			},
		}
		for i, config := range invalidConfigs {
			t.Run(fmt.Sprintf("test invalid config #%d", i+1), func(t *testing.T) {
				compassSink := compass.New(newMockHTTPClient(config, http.MethodPatch, url, compass.RequestPayload{}), testUtils.Logger)
				err := compassSink.Init(context.TODO(), plugins.Config{RawConfig: config})

				assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
			})
		}
	})
}

func TestSink(t *testing.T) {
	t.Run("should return error if compass host returns error", func(t *testing.T) {
		compassError := `{"reason":"no asset found"}`
		errMessage := "error sending data: compass returns 404: {\"reason\":\"no asset found\"}"

		// setup mock client
		url := fmt.Sprintf("%s/v1beta1/assets", host)
		client := newMockHTTPClient(map[string]interface{}{}, http.MethodPatch, url, compass.RequestPayload{})
		client.SetupResponse(404, compassError)
		ctx := context.TODO()

		compassSink := compass.New(client, testUtils.Logger)
		err := compassSink.Init(ctx, plugins.Config{RawConfig: map[string]interface{}{
			"host": host,
		}})
		if err != nil {
			t.Fatal(err)
		}

		table, err := anypb.New(&v1beta2.Table{
			Columns: nil,
		})
		require.NoError(t, err)

		data := &v1beta2.Asset{
			Data: table,
		}
		err = compassSink.Sink(ctx, []models.Record{models.NewRecord(data)})
		require.Error(t, err)
		assert.Equal(t, errMessage, err.Error())
	})

	t.Run("should return RetryError if compass returns certain status code", func(t *testing.T) {
		for _, code := range []int{500, 501, 502, 503, 504, 505} {
			t.Run(fmt.Sprintf("%d status code", code), func(t *testing.T) {
				url := fmt.Sprintf("%s/v1beta1/assets", host)
				client := newMockHTTPClient(map[string]interface{}{}, http.MethodPatch, url, compass.RequestPayload{})
				client.SetupResponse(code, `{"reason":"internal server error"}`)
				ctx := context.TODO()

				compassSink := compass.New(client, testUtils.Logger)
				err := compassSink.Init(ctx, plugins.Config{RawConfig: map[string]interface{}{
					"host": host,
				}})
				if err != nil {
					t.Fatal(err)
				}

				table, err := anypb.New(&v1beta2.Table{
					Columns: nil,
				})
				require.NoError(t, err)
				data := &v1beta2.Asset{
					Data: table,
				}
				err = compassSink.Sink(ctx, []models.Record{models.NewRecord(data)})
				require.Error(t, err)
				assert.ErrorAs(t, err, &plugins.RetryError{})
			})
		}
	})

	t.Run("should return error for various invalid labels", func(t *testing.T) {
		testData := &v1beta2.Asset{
			Urn:         "my-topic-urn",
			Name:        "my-topic",
			Service:     "kafka",
			Type:        "topic",
			Description: "topic information",
			Data: testUtils.BuildAny(t, &v1beta2.Topic{
				Attributes: utils.TryParseMapToProto(map[string]interface{}{
					"attrA": "valueAttrA",
					"attrB": "valueAttrB",
				}),
			}),
			Labels: map[string]string{
				"labelA": "valueLabelA",
				"labelB": "valueLabelB",
			},
		}
		testPayload := compass.RequestPayload{
			Asset: compass.Asset{
				URN:         "my-topic-urn",
				Name:        "my-topic",
				Service:     "kafka",
				Type:        "topic",
				Description: "topic information",
			},
		}
		invalidConfigs := []map[string]interface{}{
			{
				"host": host,
				"labels": map[string]string{
					"foo": "$attributes",
				},
			},
			{
				"host": host,
				"labels": map[string]string{
					"foo": "$attributes.12",
				},
			},
			{
				"host": host,
				"labels": map[string]string{
					"foo": "$attributes.attrC",
				},
			},
			{
				"host": host,
				"labels": map[string]string{
					"foo": "$invalid.attributes.attrC",
				},
			},
			{
				"host": host,
				"labels": map[string]string{
					"bar": "$labels.labelC",
				},
			},
		}
		for _, c := range invalidConfigs {
			client := newMockHTTPClient(c, http.MethodPatch, url, testPayload)
			client.SetupResponse(200, "")
			ctx := context.TODO()
			compassSink := compass.New(client, testUtils.Logger)
			err := compassSink.Init(ctx, plugins.Config{RawConfig: c})
			if err != nil {
				t.Fatal(err)
			}
			err = compassSink.Sink(ctx, []models.Record{models.NewRecord(testData)})
			assert.Error(t, err)
		}
	})

	successTestCases := []struct {
		description string
		data        *v1beta2.Asset
		config      map[string]interface{}
		expected    compass.RequestPayload
	}{
		{
			description: "should create the right request to compass",
			data: &v1beta2.Asset{
				Urn:         "my-topic-urn",
				Name:        "my-topic",
				Service:     "kafka",
				Type:        "topic",
				Description: "topic information",
				Url:         "http://test.com",
				Data: testUtils.BuildAny(t, &v1beta2.Table{
					Columns: []*v1beta2.Column{
						{
							Name:        "id",
							Description: "It is the ID",
							DataType:    "INT",
							IsNullable:  true,
						},
					},
				}),
				Labels: map[string]string{
					"labelA": "valueLabelA",
					"labelB": "valueLabelB",
				},
			},
			config: map[string]interface{}{
				"host": host,
			},
			expected: compass.RequestPayload{
				Asset: compass.Asset{
					URN:         "my-topic-urn",
					Name:        "my-topic",
					Service:     "kafka",
					Type:        "topic",
					URL:         "http://test.com",
					Description: "topic information",
					Data: map[string]interface{}{
						"@type": "type.googleapis.com/gotocompany.assets.v1beta2.Table",
						"columns": []map[string]interface{}{
							{
								"name":        "id",
								"description": "It is the ID",
								"data_type":   "INT",
								"is_nullable": true,
							},
						},
					},
					Labels: map[string]string{
						"labelA": "valueLabelA",
						"labelB": "valueLabelB",
					},
				},
			},
		},
		{
			description: "should build compass labels if labels is defined in config",
			data: &v1beta2.Asset{
				Urn:         "my-topic-urn",
				Name:        "my-topic",
				Service:     "kafka",
				Type:        "topic",
				Description: "topic information",
				Data: testUtils.BuildAny(t, &v1beta2.Table{
					Attributes: utils.TryParseMapToProto(map[string]interface{}{
						"attrA": "valueAttrA",
						"attrB": "valueAttrB",
					}),
				}),
			},
			config: map[string]interface{}{
				"host": host,
				"labels": map[string]string{
					"foo": "$attributes.attrA",
					"bar": "$attributes.attrB",
				},
			},
			expected: compass.RequestPayload{
				Asset: compass.Asset{
					URN:         "my-topic-urn",
					Name:        "my-topic",
					Service:     "kafka",
					Type:        "topic",
					Description: "topic information",
					Labels: map[string]string{
						"foo": "valueAttrA",
						"bar": "valueAttrB",
					},
					Data: map[string]interface{}{
						"@type": "type.googleapis.com/gotocompany.assets.v1beta2.Table",
						"attributes": map[string]interface{}{
							"attrA": "valueAttrA",
							"attrB": "valueAttrB",
						},
					},
				},
			},
		},
		{
			description: "should merge labels from assets and config",
			data: &v1beta2.Asset{
				Urn:     "my-topic-urn",
				Name:    "my-topic",
				Service: "kafka",
				Type:    "topic",
				Data: testUtils.BuildAny(t, &v1beta2.Table{
					Attributes: utils.TryParseMapToProto(map[string]interface{}{
						"newFoo": "newBar",
					}),
				}),
				Labels: map[string]string{
					"foo1": "bar1",
					"foo2": "bar2",
				},
			},
			config: map[string]interface{}{
				"host": host,
				"labels": map[string]string{
					"foo2": "$attributes.newFoo",
				},
			},
			expected: compass.RequestPayload{
				Asset: compass.Asset{
					URN:     "my-topic-urn",
					Name:    "my-topic",
					Service: "kafka",
					Type:    "topic",
					Labels: map[string]string{
						"foo1": "bar1",
						"foo2": "newBar",
					},
					Data: map[string]interface{}{
						"@type": "type.googleapis.com/gotocompany.assets.v1beta2.Table",
						"attributes": map[string]interface{}{
							"newFoo": "newBar",
						},
					},
				},
			},
		},
		{
			description: "should send upstreams if data has upstreams",
			data: &v1beta2.Asset{
				Urn:         "my-topic-urn",
				Name:        "my-topic",
				Service:     "kafka",
				Type:        "topic",
				Description: "topic information",
				Lineage: &v1beta2.Lineage{
					Upstreams: []*v1beta2.Resource{
						{
							Urn:     "urn-1",
							Type:    "type-a",
							Service: "kafka",
						},
						{
							Urn:     "urn-2",
							Type:    "type-b",
							Service: "bigquery",
						},
					},
				},
			},
			config: map[string]interface{}{
				"host": host,
			},
			expected: compass.RequestPayload{
				Asset: compass.Asset{
					URN:         "my-topic-urn",
					Name:        "my-topic",
					Service:     "kafka",
					Type:        "topic",
					Description: "topic information",
					Data:        map[string]interface{}{},
				},
				Upstreams: []compass.LineageRecord{
					{
						URN:     "urn-1",
						Type:    "type-a",
						Service: "kafka",
					},
					{
						URN:     "urn-2",
						Type:    "type-b",
						Service: "bigquery",
					},
				},
			},
		},
		{
			description: "should send downstreams if data has downstreams",
			data: &v1beta2.Asset{
				Urn:         "my-topic-urn",
				Name:        "my-topic",
				Service:     "kafka",
				Type:        "topic",
				Description: "topic information",
				Lineage: &v1beta2.Lineage{
					Downstreams: []*v1beta2.Resource{
						{
							Urn:     "urn-1",
							Type:    "type-a",
							Service: "kafka",
						},
						{
							Urn:     "urn-2",
							Type:    "type-b",
							Service: "bigquery",
						},
					},
				},
			},
			config: map[string]interface{}{
				"host": host,
			},
			expected: compass.RequestPayload{
				Asset: compass.Asset{
					URN:         "my-topic-urn",
					Name:        "my-topic",
					Service:     "kafka",
					Type:        "topic",
					Description: "topic information",
					Data:        map[string]interface{}{},
				},
				Downstreams: []compass.LineageRecord{
					{
						URN:     "urn-1",
						Type:    "type-a",
						Service: "kafka",
					},
					{
						URN:     "urn-2",
						Type:    "type-b",
						Service: "bigquery",
					},
				},
			},
		},
		{
			description: "should send owners if data has ownership",
			data: &v1beta2.Asset{
				Urn:         "my-topic-urn",
				Name:        "my-topic",
				Service:     "kafka",
				Type:        "topic",
				Description: "topic information",
				Owners: []*v1beta2.Owner{
					{
						Urn:   "urn-1",
						Name:  "owner-a",
						Role:  "role-a",
						Email: "email-1",
					},
					{
						Urn:   "urn-2",
						Name:  "owner-b",
						Role:  "role-b",
						Email: "email-2",
					},
					{
						Urn:   "urn-3",
						Name:  "owner-c",
						Role:  "role-c",
						Email: "email-3",
					},
				},
			},
			config: map[string]interface{}{
				"host": host,
			},
			expected: compass.RequestPayload{
				Asset: compass.Asset{
					URN:         "my-topic-urn",
					Name:        "my-topic",
					Service:     "kafka",
					Type:        "topic",
					Description: "topic information",
					Data:        map[string]interface{}{},
					Owners: []compass.Owner{
						{
							URN:   "urn-1",
							Name:  "owner-a",
							Role:  "role-a",
							Email: "email-1",
						},
						{
							URN:   "urn-2",
							Name:  "owner-b",
							Role:  "role-b",
							Email: "email-2",
						},
						{
							URN:   "urn-3",
							Name:  "owner-c",
							Role:  "role-c",
							Email: "email-3",
						},
					},
				},
			},
		},
		{
			description: "should send headers if get populated in config",
			data: &v1beta2.Asset{
				Urn:         "my-topic-urn",
				Name:        "my-topic",
				Service:     "kafka",
				Type:        "topic",
				Description: "topic information",
			},
			config: map[string]interface{}{
				"host": host,
				"headers": map[string]string{
					"Key1": "value11, value12",
					"Key2": "value2",
				},
			},
			expected: compass.RequestPayload{
				Asset: compass.Asset{
					URN:         "my-topic-urn",
					Name:        "my-topic",
					Service:     "kafka",
					Type:        "topic",
					Description: "topic information",
					Data:        map[string]interface{}{},
				},
			},
		},
	}

	for _, tc := range successTestCases {
		t.Run(tc.description, func(t *testing.T) {
			client := newMockHTTPClient(tc.config, http.MethodPatch, url, tc.expected)
			client.SetupResponse(200, "")
			ctx := context.TODO()

			compassSink := compass.New(client, testUtils.Logger)
			err := compassSink.Init(ctx, plugins.Config{RawConfig: tc.config})
			require.NoError(t, err)

			err = compassSink.Sink(ctx, []models.Record{models.NewRecord(tc.data)})
			assert.NoError(t, err)

			client.Assert(t)
		})
	}
}

type mockHTTPClient struct {
	URL            string
	Method         string
	Headers        map[string]string
	RequestPayload compass.RequestPayload
	ResponseJSON   string
	ResponseStatus int
	req            *http.Request
}

func newMockHTTPClient(config map[string]interface{}, method, url string, payload compass.RequestPayload) *mockHTTPClient {
	headersMap := map[string]string{}
	if headersItf, ok := config["headers"]; ok {
		headersMap = headersItf.(map[string]string)
	}
	return &mockHTTPClient{
		Method:         method,
		URL:            url,
		Headers:        headersMap,
		RequestPayload: payload,
	}
}

func (m *mockHTTPClient) SetupResponse(statusCode int, json string) {
	m.ResponseStatus = statusCode
	m.ResponseJSON = json
}

func (m *mockHTTPClient) Do(req *http.Request) (res *http.Response, err error) {
	m.req = req

	res = &http.Response{
		// default values
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		StatusCode:    m.ResponseStatus,
		Request:       req,
		Header:        make(http.Header),
		ContentLength: int64(len(m.ResponseJSON)),
		Body:          io.NopCloser(bytes.NewBufferString(m.ResponseJSON)),
	}

	return
}

func (m *mockHTTPClient) Assert(t *testing.T) {
	assert.Equal(t, m.Method, m.req.Method)
	actualURL := fmt.Sprintf(
		"%s://%s%s",
		m.req.URL.Scheme,
		m.req.URL.Host,
		m.req.URL.Path,
	)
	assert.Equal(t, m.URL, actualURL)

	headersMap := map[string]string{}
	for hdrKey, hdrVals := range m.req.Header {
		headersMap[hdrKey] = strings.Join(hdrVals, ",")
	}
	assert.Equal(t, m.Headers, headersMap)
	var bodyBytes = []byte("")
	if m.req.Body != nil {
		var err error
		bodyBytes, err = io.ReadAll(m.req.Body)
		if err != nil {
			t.Error(err)
		}
	}

	expectedBytes, err := json.Marshal(m.RequestPayload)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, string(expectedBytes), string(bodyBytes))
}
