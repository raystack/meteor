package shield_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	testUtils "github.com/odpf/meteor/test/utils"
	"github.com/odpf/meteor/utils"
	"github.com/pkg/errors"

	"github.com/odpf/meteor/models"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/sinks/shield"
	"github.com/stretchr/testify/assert"
)

var (
	host = "http://shield.com"
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
				url := fmt.Sprintf("%s/admin/v1beta1/users/user@odpf.com", host)
				shieldSink := shield.New(newMockHTTPClient(config, http.MethodPatch, url, shield.RequestPayload{}), testUtils.Logger)
				err := shieldSink.Init(context.TODO(), plugins.Config{RawConfig: config})

				assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
			})
		}
	})
}

func TestSink(t *testing.T) {
	t.Run("should return error if shield host returns error", func(t *testing.T) {
		shieldError := `{"reason":"no asset found"}`
		errMessage := "error sending data: shield returns 404: {\"reason\":\"no asset found\"}"

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

		// setup mock client
		url := fmt.Sprintf("%s/admin/v1beta1/users/user@odpf.com", host)
		client := newMockHTTPClient(map[string]interface{}{}, http.MethodPut, url, shield.RequestPayload{})
		client.SetupResponse(404, shieldError)
		ctx := context.TODO()

		shieldSink := shield.New(client, testUtils.Logger)
		err = shieldSink.Init(ctx, plugins.Config{RawConfig: map[string]interface{}{
			"host": host,
		}})
		if err != nil {
			t.Fatal(err)
		}

		err = shieldSink.Sink(ctx, []models.Record{models.NewRecord(data)})
		require.Error(t, err)
		assert.Equal(t, errMessage, err.Error())
	})

	t.Run("should return RetryError if shield returns certain status code", func(t *testing.T) {
		for _, code := range []int{500, 501, 502, 503, 504, 505} {
			t.Run(fmt.Sprintf("%d status code", code), func(t *testing.T) {
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

				// setup mock client
				url := fmt.Sprintf("%s/admin/v1beta1/users/user@odpf.com", host)

				client := newMockHTTPClient(map[string]interface{}{}, http.MethodPut, url, shield.RequestPayload{})
				client.SetupResponse(code, `{"reason":"internal server error"}`)
				ctx := context.TODO()

				shieldSink := shield.New(client, testUtils.Logger)
				err = shieldSink.Init(ctx, plugins.Config{RawConfig: map[string]interface{}{
					"host": host,
				}})
				if err != nil {
					t.Fatal(err)
				}

				err = shieldSink.Sink(ctx, []models.Record{models.NewRecord(data)})
				require.Error(t, err)
				assert.True(t, errors.Is(err, plugins.RetryError{}))
			})
		}
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
				wantErr: errors.Wrap(errors.New("name must be a string"), "failed to build shield payload"),
			},
			{
				User: &v1beta2.User{
					FullName:   "John Doe",
					Email:      "",
					Attributes: utils.TryParseMapToProto(map[string]interface{}{}),
				},
				wantErr: errors.Wrap(errors.New("email must be a string"), "failed to build shield payload"),
			},
			{
				User: &v1beta2.User{
					FullName: "John Doe",
					Email:    "john.doe@odpf.com",
				},
				wantErr: errors.Wrap(errors.New("attributes must be a map[string]interface{}"), "failed to build shield payload"),
			},
		}

		for _, d := range testData {
			user, _ := anypb.New(d.User)
			data := &v1beta2.Asset{
				Data: user,
			}

			// setup mock client
			url := fmt.Sprintf("%s/admin/v1beta1/users/user@odpf.com", host)

			client := newMockHTTPClient(map[string]interface{}{}, http.MethodPut, url, shield.RequestPayload{})
			ctx := context.TODO()

			shieldSink := shield.New(client, testUtils.Logger)
			err := shieldSink.Init(ctx, plugins.Config{RawConfig: map[string]interface{}{
				"host": host,
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
		user, _ := anypb.New(&v1beta2.User{
			FullName: "John Doe",
			Email:    "john.doe@odpf.com",
			Attributes: utils.TryParseMapToProto(map[string]interface{}{
				"org_unit_path": "/",
				"aliases":       "doe.john@odpf.com,johndoe@odpf.com",
			}),
		})
		data := &v1beta2.Asset{
			Data: user,
		}

		// setup mock client
		url := fmt.Sprintf("%s/admin/v1beta1/users/user@odpf.com", host)

		client := newMockHTTPClient(map[string]interface{}{}, http.MethodPut, url, shield.RequestPayload{})
		ctx := context.TODO()
		client.SetupResponse(200, "")

		shieldSink := shield.New(client, testUtils.Logger)
		err := shieldSink.Init(ctx, plugins.Config{RawConfig: map[string]interface{}{
			"host": host,
		}})
		if err != nil {
			t.Fatal(err)
		}

		err = shieldSink.Sink(ctx, []models.Record{models.NewRecord(data)})
		assert.Equal(t, nil, err)
	})
}

type mockHTTPClient struct {
	URL            string
	Method         string
	Headers        map[string]string
	RequestPayload shield.RequestPayload
	ResponseJSON   string
	ResponseStatus int
	req            *http.Request
}

func newMockHTTPClient(config map[string]interface{}, method, url string, payload shield.RequestPayload) *mockHTTPClient {
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
		Body:          ioutil.NopCloser(bytes.NewBufferString(m.ResponseJSON)),
	}

	return
}
