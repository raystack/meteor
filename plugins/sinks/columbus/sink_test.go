package columbus_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/models/odpf/assets/facets"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/sinks/columbus"
	"github.com/odpf/meteor/test"
	"github.com/stretchr/testify/assert"
)

var (
	host = "http://columbus.com"
)

// sample metadata
var (
	topic = &assets.Topic{
		Resource: &common.Resource{
			Urn:  "my-topic-urn",
			Name: "my-topic",
		},
		Ownership: &facets.Ownership{
			Owners: []*facets.Owner{
				{Name: "admin-A"},
			},
		},
	}
	requestPayload = `[{"resource":{"urn":"my-topic-urn","name":"my-topic"},"ownership":{"owners":[{"name":"admin-A"}]}}]`
	columbusType   = "my-type"
	url            = fmt.Sprintf("%s/v1/types/%s/records", host, columbusType)
)

func TestInit(t *testing.T) {
	t.Run("should return InvalidConfigError on invalid config", func(t *testing.T) {
		invalidConfigs := []map[string]interface{}{
			{
				"host": "",
				"type": "columbus-type",
			},
			{
				"host": host,
				"type": "",
			},
		}
		for i, config := range invalidConfigs {
			t.Run(fmt.Sprintf("test invalid config #%d", i+1), func(t *testing.T) {
				columbusSink := columbus.New(newmockHTTPClient(http.MethodGet, url, requestPayload), test.Logger)
				err := columbusSink.Init(context.TODO(), config)

				assert.Equal(t, plugins.InvalidConfigError{Type: plugins.PluginTypeSink}, err)
			})
		}
	})
}

func TestSink(t *testing.T) {
	t.Run("should create the right request to columbus", func(t *testing.T) {
		client := newmockHTTPClient(http.MethodPut, url, requestPayload)
		client.SetupResponse(200, "")
		ctx := context.TODO()

		columbusSink := columbus.New(client, test.Logger)
		err := columbusSink.Init(ctx, map[string]interface{}{
			"host": host,
			"type": columbusType,
		})
		if err != nil {
			t.Fatal(err)
		}

		columbusSink.Sink(ctx, []models.Record{models.NewRecord(topic)})
		client.Assert(t)
	})

	t.Run("should return error if columbus host returns error", func(t *testing.T) {
		columbusError := `{"reason":"no such type: \"my-type\""}`
		errMessage := "error sending data: columbus returns 404: {\"reason\":\"no such type: \\\"my-type\\\"\"}"

		// setup mock client
		url := fmt.Sprintf("%s/v1/types/my-type/records", host)
		client := newmockHTTPClient(http.MethodPut, url, requestPayload)
		client.SetupResponse(404, columbusError)
		ctx := context.TODO()

		columbusSink := columbus.New(client, test.Logger)
		err := columbusSink.Init(ctx, map[string]interface{}{
			"host": host,
			"type": "my-type",
		})
		if err != nil {
			t.Fatal(err)
		}

		err = columbusSink.Sink(ctx, []models.Record{models.NewRecord(topic)})
		assert.Equal(t, errMessage, err.Error())

		client.Assert(t)
	})

	t.Run("should return no error if columbus returns 200", func(t *testing.T) {
		// setup mock client
		client := newmockHTTPClient(http.MethodPut, url, requestPayload)
		client.SetupResponse(200, `{"success": true}`)
		ctx := context.TODO()

		columbusSink := columbus.New(client, test.Logger)
		err := columbusSink.Init(ctx, map[string]interface{}{
			"host": host,
			"type": "my-type",
		})
		if err != nil {
			t.Fatal(err)
		}

		err = columbusSink.Sink(ctx, []models.Record{models.NewRecord(topic)})
		assert.NoError(t, err)
		client.Assert(t)
	})

	t.Run("should map fields using mapper from config", func(t *testing.T) {
		metadata := &assets.Topic{
			Resource: &common.Resource{
				Urn:  "test-urn",
				Name: "test-name",
			},
			Description: "test-desc",
		}
		mapping := map[string]string{
			"fieldA": "resource.urn",
			"fieldB": "resource.name",
			"fieldC": "description",
		}
		requestPayload := `[{"description":"test-desc","fieldA":"test-urn","fieldB":"test-name","fieldC":"test-desc","resource":{"name":"test-name","urn":"test-urn"}}]`

		client := newmockHTTPClient(http.MethodPut, url, requestPayload)
		client.SetupResponse(200, "")

		ctx := context.TODO()
		columbusSink := columbus.New(client, test.Logger)
		err := columbusSink.Init(ctx, map[string]interface{}{
			"host":    host,
			"type":    columbusType,
			"mapping": mapping,
		})
		if err != nil {
			t.Fatal(err)
		}

		err = columbusSink.Sink(ctx, []models.Record{models.NewRecord(metadata)})
		assert.NoError(t, err)
		client.Assert(t)
	})
}

type mockHTTPClient struct {
	URL                string
	Method             string
	RequestPayloadJSON string
	ResponseJSON       string
	ResponseStatus     int
	req                *http.Request
}

func newmockHTTPClient(method, url string, payloadJSON string) *mockHTTPClient {
	return &mockHTTPClient{
		Method:             method,
		URL:                url,
		RequestPayloadJSON: payloadJSON,
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

	assert.Equal(t, string(m.RequestPayloadJSON), string(bodyBytes))
}
