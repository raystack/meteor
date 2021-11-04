package columbus_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	testUtils "github.com/odpf/meteor/test/utils"
	"github.com/odpf/meteor/utils"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/models/odpf/assets/facets"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/sinks/columbus"
	"github.com/stretchr/testify/assert"
)

var (
	host = "http://columbus.com"
)

// sample metadata
var (
	columbusType = "my-type"
	url          = fmt.Sprintf("%s/v1/types/%s/records", host, columbusType)
)

func TestInit(t *testing.T) {
	t.Run("should return InvalidConfigError on invalid config", func(t *testing.T) {
		invalidConfigs := []map[string]interface{}{
			{
				"host": "",
			},
			{
				"host": host,
				"type": "",
			},
		}
		for i, config := range invalidConfigs {
			t.Run(fmt.Sprintf("test invalid config #%d", i+1), func(t *testing.T) {
				columbusSink := columbus.New(newMockHTTPClient(http.MethodGet, url, []columbus.Record{}), testUtils.Logger)
				err := columbusSink.Init(context.TODO(), config)

				assert.Equal(t, plugins.InvalidConfigError{Type: plugins.PluginTypeSink}, err)
			})
		}
	})
}

func TestSink(t *testing.T) {
	t.Run("should return error if columbus host returns error", func(t *testing.T) {
		columbusError := `{"reason":"no such type: \"my-type\""}`
		errMessage := "error sending data: columbus returns 404: {\"reason\":\"no such type: \\\"my-type\\\"\"}"

		// setup mock client
		url := fmt.Sprintf("%s/v1/types/my-type/records", host)
		client := newMockHTTPClient(http.MethodPut, url, []columbus.Record{})
		client.SetupResponse(404, columbusError)
		ctx := context.TODO()

		columbusSink := columbus.New(client, testUtils.Logger)
		err := columbusSink.Init(ctx, map[string]interface{}{
			"host": host,
			"type": "my-type",
		})
		if err != nil {
			t.Fatal(err)
		}

		data := &assets.Topic{Resource: &common.Resource{}}
		err = columbusSink.Sink(ctx, []models.Record{models.NewRecord(data)})
		assert.Equal(t, errMessage, err.Error())
	})

	t.Run("should return RetryError if columbus returns certain status code", func(t *testing.T) {
		for _, code := range []int{500, 501, 502, 503, 504, 505} {
			t.Run(fmt.Sprintf("%d status code", code), func(t *testing.T) {
				url := fmt.Sprintf("%s/v1/types/my-type/records", host)
				client := newMockHTTPClient(http.MethodPut, url, []columbus.Record{})
				client.SetupResponse(code, `{"reason":"internal server error"}`)
				ctx := context.TODO()

				columbusSink := columbus.New(client, testUtils.Logger)
				err := columbusSink.Init(ctx, map[string]interface{}{
					"host": host,
					"type": "my-type",
				})
				if err != nil {
					t.Fatal(err)
				}

				data := &assets.Topic{Resource: &common.Resource{}}
				err = columbusSink.Sink(ctx, []models.Record{models.NewRecord(data)})
				assert.True(t, errors.Is(err, plugins.RetryError{}))
			})
		}
	})

	successTestCases := []struct {
		description string
		data        models.Metadata
		config      map[string]interface{}
		expected    columbus.Record
	}{
		{
			description: "should create the right request to columbus",
			data: &assets.Topic{
				Resource: &common.Resource{
					Urn:     "my-topic-urn",
					Name:    "my-topic",
					Service: "kafka",
				},
			},
			config: map[string]interface{}{
				"host": host,
				"type": columbusType,
			},
			expected: columbus.Record{
				Urn:     "my-topic-urn",
				Name:    "my-topic",
				Service: "kafka",
			},
		},
		{
			description: "should build columbus labels if labels is defined in config",
			data: &assets.Topic{
				Resource: &common.Resource{
					Urn:     "my-topic-urn",
					Name:    "my-topic",
					Service: "kafka",
				},
				Properties: &facets.Properties{
					Attributes: utils.TryParseMapToProto(map[string]interface{}{
						"attrA": "valueAttrA",
						"attrB": "valueAttrB",
					}),
					Labels: map[string]string{
						"labelA": "valueLabelA",
						"labelB": "valueLabelB",
					},
				},
			},
			config: map[string]interface{}{
				"host": host,
				"type": columbusType,
				"labels": map[string]string{
					"foo": "$properties.attributes.attrB",
					"bar": "$properties.labels.labelA",
				},
			},
			expected: columbus.Record{
				Urn:     "my-topic-urn",
				Name:    "my-topic",
				Service: "kafka",
				Labels: map[string]string{
					"foo": "valueAttrB",
					"bar": "valueLabelA",
				},
			},
		},
		{
			description: "should send upstreams if data has upstreams",
			data: &assets.Topic{
				Resource: &common.Resource{
					Urn:     "my-topic-urn",
					Name:    "my-topic",
					Service: "kafka",
				},
				Lineage: &facets.Lineage{
					Upstreams: []*common.Resource{
						{
							Urn:  "urn-1",
							Type: "type-a",
						},
						{
							Urn:  "urn-2",
							Type: "type-b",
						},
					},
				},
			},
			config: map[string]interface{}{
				"host": host,
				"type": columbusType,
			},
			expected: columbus.Record{
				Urn:     "my-topic-urn",
				Name:    "my-topic",
				Service: "kafka",
				Upstreams: []columbus.LineageRecord{
					{
						Urn:  "urn-1",
						Type: "type-a",
					},
					{
						Urn:  "urn-2",
						Type: "type-b",
					},
				},
			},
		},
		{
			description: "should send downstreams if data has downstreams",
			data: &assets.Topic{
				Resource: &common.Resource{
					Urn:     "my-topic-urn",
					Name:    "my-topic",
					Service: "kafka",
				},
				Lineage: &facets.Lineage{
					Downstreams: []*common.Resource{
						{
							Urn:  "urn-1",
							Type: "type-a",
						},
						{
							Urn:  "urn-2",
							Type: "type-b",
						},
					},
				},
			},
			config: map[string]interface{}{
				"host": host,
				"type": columbusType,
			},
			expected: columbus.Record{
				Urn:     "my-topic-urn",
				Name:    "my-topic",
				Service: "kafka",
				Downstreams: []columbus.LineageRecord{
					{
						Urn:  "urn-1",
						Type: "type-a",
					},
					{
						Urn:  "urn-2",
						Type: "type-b",
					},
				},
			},
		},
	}

	for _, tc := range successTestCases {
		t.Run(tc.description, func(t *testing.T) {
			tc.expected.Data = tc.data
			payload := []columbus.Record{tc.expected}

			client := newMockHTTPClient(http.MethodPut, url, payload)
			client.SetupResponse(200, "")
			ctx := context.TODO()

			columbusSink := columbus.New(client, testUtils.Logger)
			err := columbusSink.Init(ctx, tc.config)
			if err != nil {
				t.Fatal(err)
			}

			err = columbusSink.Sink(ctx, []models.Record{models.NewRecord(tc.data)})
			assert.NoError(t, err)

			client.Assert(t)
		})
	}
}

type mockHTTPClient struct {
	URL            string
	Method         string
	RequestPayload []columbus.Record
	ResponseJSON   string
	ResponseStatus int
	req            *http.Request
}

func newMockHTTPClient(method, url string, payload []columbus.Record) *mockHTTPClient {
	return &mockHTTPClient{
		Method:         method,
		URL:            url,
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
		Body:          ioutil.NopCloser(bytes.NewBufferString(m.ResponseJSON)),
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

	var bodyBytes = []byte("")
	if m.req.Body != nil {
		var err error
		bodyBytes, err = ioutil.ReadAll(m.req.Body)
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
