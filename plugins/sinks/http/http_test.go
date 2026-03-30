//go:build plugins
// +build plugins

package http_test

import (
	"context"
	_ "embed"
	"fmt"
	"net/http"
	"testing"

	"github.com/dnaeon/go-vcr/v2/recorder"
	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	h "github.com/raystack/meteor/plugins/sinks/http"
	testutils "github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/structpb"
)

//go:embed README.md
var summary string

var success_code int = 200

func TestSink(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		httpSink := h.New(&http.Client{}, testutils.Logger)
		config := map[string]any{
			"success_code": success_code,
			"method":       "POST",
			"headers": map[string]string{
				"Content-Type": "application/json",
				"Accept":       "application/json",
			},
		}
		err := httpSink.Init(context.TODO(), plugins.Config{RawConfig: config})
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should return no error for valid config, without optional values", func(t *testing.T) {
		httpSink := h.New(&http.Client{}, testutils.Logger)
		config := map[string]any{
			"url":    "http://sitename.com",
			"method": "POST",
		}
		err := httpSink.Init(context.TODO(), plugins.Config{RawConfig: config})
		assert.NoError(t, err)
	})

	t.Run("should return error for status code when not success", func(t *testing.T) {
		r, err := recorder.New(fmt.Sprintf("fixtures/response_%d", 404))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			err := r.Stop()
			if err != nil {
				t.Fatal(err)
			}
		}()
		httpSink := h.New(&http.Client{Transport: r}, testutils.Logger)
		config := map[string]any{
			"success_code": success_code,
			"url":          "http://127.0.0.1:54927",
			"method":       "PATCH",
			"headers": map[string]string{
				"Content-Type": "application/json",
				"Accept":       "application/json",
			},
		}
		err = httpSink.Init(context.TODO(), plugins.Config{RawConfig: config})
		assert.NoError(t, err)
		defer httpSink.Close()
		err = httpSink.Sink(context.TODO(), getExpectedVal(t))
		assert.Error(t, err)

		// change value of url in config
		config["url"] = "https://random-incorrect-url.raystack.com"
		err = httpSink.Init(context.TODO(), plugins.Config{RawConfig: config})
		assert.NoError(t, err)
		err = httpSink.Sink(context.TODO(), getExpectedVal(t))
		assert.Error(t, err)

		// change value of method in config
		config["method"] = "RANDOM"
		config["url"] = "http://127.0.0.1:54927"
		err = httpSink.Init(context.TODO(), plugins.Config{RawConfig: config})
		assert.NoError(t, err)
		err = httpSink.Sink(context.TODO(), getExpectedVal(t))
		assert.Error(t, err)
	})

	t.Run("should return retry error for error code 5xx", func(t *testing.T) {
		port := 54931
		for _, code := range []int{500, 501, 502, 503, 504, 505} {
			t.Run(fmt.Sprintf("should retry for status code %d", code), func(t *testing.T) {
				r, err := recorder.New(fmt.Sprintf("fixtures/response_%d", code))
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					err := r.Stop()
					if err != nil {
						t.Fatal(err)
					}
				}()
				httpSink := h.New(&http.Client{Transport: r}, testutils.Logger)
				config := map[string]any{
					"success_code": success_code,
					"url":          fmt.Sprintf("http://127.0.0.1:%d", port),
					"method":       "POST",
					"headers": map[string]string{
						"Content-Type": "application/json",
						"Accept":       "application/json",
					},
				}
				err = httpSink.Init(context.TODO(), plugins.Config{RawConfig: config})
				assert.NoError(t, err)
				defer httpSink.Close()
				err = httpSink.Sink(context.TODO(), getExpectedVal(t))
				assert.ErrorAs(t, err, &plugins.RetryError{})
				port += 2
			})
		}
	})

	t.Run("should return no error for correct status code in response", func(t *testing.T) {
		r, err := recorder.New("fixtures/response")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			err := r.Stop()
			if err != nil {
				t.Fatal(err)
			}
		}()
		httpSink := h.New(&http.Client{Transport: r}, testutils.Logger)
		config := map[string]any{
			"success_code": success_code,
			"url":          "http://127.0.0.1:54943",
			"method":       "POST",
			"headers": map[string]string{
				"Content-Type": "application/json",
				"Accept":       "application/json",
			},
		}
		err = httpSink.Init(context.TODO(), plugins.Config{RawConfig: config})
		assert.NoError(t, err)
		defer httpSink.Close()
		err = httpSink.Sink(context.TODO(), getExpectedVal(t))
		assert.NoError(t, err)
	})

	t.Run("should return no error when using templates", func(t *testing.T) {
		r, err := recorder.New("fixtures/response_with_script")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			err := r.Stop()
			if err != nil {
				t.Fatal(err)
			}
		}()
		httpSink := h.New(&http.Client{Transport: r}, testutils.Logger)
		config := map[string]any{
			"success_code": success_code,
			"url":          "http://127.0.0.1:54943/{{ .Type }}/{{ .Urn }}",
			"method":       "POST",
			"headers": map[string]string{
				"Content-Type": "application/json",
				"Accept":       "application/json",
			},
			"script": map[string]any{
				"engine": "tengo",
				"source": `
					payload := {
						details: {
							some_key: asset.urn,
							another_key: asset.name
						}
					}
					sink(payload)
				`,
			},
		}
		err = httpSink.Init(context.TODO(), plugins.Config{RawConfig: config})
		assert.NoError(t, err)
		defer httpSink.Close()
		err = httpSink.Sink(context.TODO(), getExpectedVal(t))
		assert.NoError(t, err)
	})
}

func getExpectedVal(t *testing.T) []models.Record {
	props1, err := structpb.NewStruct(map[string]any{
		"columns": []any{
			map[string]any{
				"name":      "SomeStr",
				"data_type": "text",
			},
		},
		"profile": map[string]any{
			"total_rows": float64(1),
		},
	})
	if err != nil {
		t.Fatal("error creating properties for test:", err)
	}
	props2, err := structpb.NewStruct(map[string]any{
		"columns": []any{
			map[string]any{
				"name":      "SomeStr",
				"data_type": "text",
			},
		},
		"profile": map[string]any{
			"total_rows": float64(1),
		},
	})
	if err != nil {
		t.Fatal("error creating properties for test:", err)
	}
	return []models.Record{
		models.NewRecord(&meteorv1beta1.Entity{
			Urn:        "elasticsearch.index1",
			Name:       "index1",
			Type:       "table",
			Properties: props1,
		}),
		models.NewRecord(&meteorv1beta1.Entity{
			Urn:        "elasticsearch.index2",
			Name:       "index2",
			Type:       "table",
			Properties: props2,
		}),
	}
}

func TestInfo(t *testing.T) {
	info := h.New(&http.Client{}, testutils.Logger).Info()
	assert.Equal(t, summary, info.Summary)
}
