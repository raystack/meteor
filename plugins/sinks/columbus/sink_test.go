package columbus_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"testing"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/sinks/columbus"
	"github.com/odpf/meteor/proto/odpf/entities/facets"
	"github.com/odpf/meteor/proto/odpf/entities/resources"
	"github.com/stretchr/testify/assert"
)

var (
	host = "http://columbus.com"
)

func TestSink(t *testing.T) {
	// sample metadata
	var (
		topic = resources.Topic{
			Urn:  "my-topic-urn",
			Name: "my-topic",
			Ownership: &facets.Ownership{
				Owners: []*facets.Owner{
					{Name: "admin-A"},
				},
			},
		}
		requestPayload = `[{"urn":"my-topic-urn","name":"my-topic","ownership":{"owners":[{"name":"admin-A"}]}}]`
		columbusType   = "my-type"
		url            = fmt.Sprintf("%s/v1/types/%s/records", host, columbusType)
	)

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
				columbusSink := columbus.New(newMockHttpClient(http.MethodGet, url, requestPayload))
				err := columbusSink.Sink(context.TODO(), config, make(<-chan interface{}))

				assert.Equal(t, plugins.InvalidConfigError{Type: plugins.PluginTypeSink}, err)
			})
		}
	})

	t.Run("should create the right request to columbus", func(t *testing.T) {
		client := newMockHttpClient(http.MethodPut, url, requestPayload)
		client.SetupResponse(200, "")

		in := make(chan interface{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			columbusSink := columbus.New(client)
			columbusSink.Sink(context.TODO(), map[string]interface{}{
				"host": host,
				"type": columbusType,
			}, in)

			client.Assert(t)

			wg.Done()
		}()

		in <- topic
		close(in)
		wg.Wait()
	})

	t.Run("should return error if columbus host returns error", func(t *testing.T) {
		columbusError := `{"reason":"no such type: \"my-type\""}`
		expectedErr := errors.New("columbus returns 404: {\"reason\":\"no such type: \\\"my-type\\\"\"}")

		// setup mock client
		url := fmt.Sprintf("%s/v1/types/my-type/records", host)
		client := newMockHttpClient(http.MethodPut, url, requestPayload)
		client.SetupResponse(404, columbusError)

		in := make(chan interface{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			columbusSink := columbus.New(client)
			err := columbusSink.Sink(context.TODO(), map[string]interface{}{
				"host": host,
				"type": "my-type",
			}, in)

			assert.Equal(t, expectedErr, err)
			client.Assert(t)

			wg.Done()
		}()

		in <- topic
		wg.Wait()
	})

	t.Run("should return no error if columbus returns 200", func(t *testing.T) {
		// setup mock client
		client := newMockHttpClient(http.MethodPut, url, requestPayload)
		client.SetupResponse(200, `{"success": true}`)

		in := make(chan interface{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			columbusSink := columbus.New(client)
			err := columbusSink.Sink(context.TODO(), map[string]interface{}{
				"host": host,
				"type": "my-type",
			}, in)

			assert.NoError(t, err)
			client.Assert(t)

			wg.Done()
		}()

		in <- topic
		close(in)
		wg.Wait()
	})

	t.Run("should map fields using mapper from config", func(t *testing.T) {
		metadata := resources.Topic{
			Urn:         "test-urn",
			Name:        "test-name",
			Description: "test-description",
		}
		mapping := map[string]string{
			"Urn":         "fieldA",
			"Name":        "fieldB",
			"Description": "fieldC",
		}
		requestPayload := `[{"fieldA":"test-urn","fieldB":"test-name","fieldC":"test-description"}]`

		client := newMockHttpClient(http.MethodPut, url, requestPayload)
		client.SetupResponse(200, "")

		in := make(chan interface{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			columbusSink := columbus.New(client)
			columbusSink.Sink(context.TODO(), map[string]interface{}{
				"host":    host,
				"type":    columbusType,
				"mapping": mapping,
			}, in)

			client.Assert(t)

			wg.Done()
		}()

		in <- metadata
		close(in)
		wg.Wait()
	})
}

type mockHttpClient struct {
	URL                string
	Method             string
	RequestPayloadJSON string
	ResponseJSON       string
	ResponseStatus     int
	req                *http.Request
}

func newMockHttpClient(method, url string, payloadJSON string) *mockHttpClient {
	return &mockHttpClient{
		Method:             method,
		URL:                url,
		RequestPayloadJSON: payloadJSON,
	}
}

func (m *mockHttpClient) SetupResponse(statusCode int, json string) {
	m.ResponseStatus = statusCode
	m.ResponseJSON = json
}

func (m *mockHttpClient) Do(req *http.Request) (res *http.Response, err error) {
	m.req = req

	res = &http.Response{
		// default values
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		StatusCode:    m.ResponseStatus,
		Request:       req,
		Header:        make(http.Header, 0),
		ContentLength: int64(len(m.ResponseJSON)),
		Body:          ioutil.NopCloser(bytes.NewBufferString(m.ResponseJSON)),
	}

	return
}

func (m *mockHttpClient) Assert(t *testing.T) {
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
