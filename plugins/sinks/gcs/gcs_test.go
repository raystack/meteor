//go:build plugins
// +build plugins

package gcs_test

import (
	"context"
	"testing"

	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	g "github.com/goto/meteor/plugins/sinks/gcs"
	testUtils "github.com/goto/meteor/test/utils"
	"github.com/goto/meteor/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/anypb"
)

var validConfig = map[string]interface{}{
	"project_id":             "google-project-id",
	"url":                    "gcs://bucket_name/target_folder",
	"object_prefix":          "github-users",
	"service_account_base64": "base 64 encoded key",
}

func TestInit(t *testing.T) {

	t.Run("should return error if config is invalid", func(t *testing.T) {
		gcsSink := g.New(testUtils.Logger)
		actualError := gcsSink.Init(context.TODO(), plugins.Config{RawConfig: map[string]interface{}{
			"project_id": "",
		}})
		assert.ErrorAs(t, actualError, &plugins.InvalidConfigError{})
	})

	t.Run("should retun error if service account json and service account base64 missing", func(t *testing.T) {

		gcsSink := g.New(testUtils.Logger)
		actualError := gcsSink.Init(context.TODO(), plugins.Config{RawConfig: map[string]interface{}{
			"project_id": "google-project-id",
			"url":        "gcs://bucket_name/target_folder",
		}})
		assert.ErrorContains(t, actualError, "credentials are not specified, failed to create client")
	})

	t.Run("should retun error if unable to decode base64 service account key", func(t *testing.T) {

		gcsSink := g.New(testUtils.Logger)
		actualError := gcsSink.Init(context.TODO(), plugins.Config{RawConfig: map[string]interface{}{
			"project_id":             "google-project-id",
			"url":                    "gcs://bucket_name/target_folder",
			"service_account_base64": "----", // invalid
		}})
		assert.ErrorContains(t, actualError, "failed to decode base64 service account")
	})
}

func TestSink(t *testing.T) {

	t.Run("should write data in bucket and return nil error on success", func(t *testing.T) {
		u := &v1beta2.User{
			FullName: "John Doe",
			Email:    "john.doe@gotocompany.com",
			Attributes: utils.TryParseMapToProto(map[string]interface{}{
				"org_unit_path": "/",
				"aliases":       "doe.john@gotocompany.com,johndoe@gotocompany.com",
			}),
		}
		user, _ := anypb.New(u)
		data := &v1beta2.Asset{
			Data: user,
		}
		jsonBytes, _ := models.ToJSON(data)

		ctx := context.TODO()
		writer := new(mockWriter)
		writer.On("WriteData", jsonBytes).Return(nil)
		writer.On("WriteData", []byte("\n")).Return(nil)

		gcsSink := g.New(testUtils.Logger)

		err := gcsSink.Init(context.TODO(), plugins.Config{RawConfig: map[string]interface{}{
			"project_id":           "google-project-id",
			"url":                  "gcs://bucket_name/target_folder",
			"service_account_json": `{"type": "service_account"}`,
		}})
		if err != nil {
			t.Fatal(err)
		}

		err = gcsSink.Sink(ctx, []models.Record{models.NewRecord(data)})

		assert.NoError(t, err)
	})
}

type mockWriter struct {
	mock.Mock
}

func (m *mockWriter) WriteData(jsonBytes []byte) error {
	args := m.Called(jsonBytes)

	return args.Error(0)
}

func (m *mockWriter) Close() error {
	args := m.Called()

	return args.Error(0)
}
