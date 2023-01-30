//go:build plugins
// +build plugins
package gcs_test

import(
	"testing"
	"context"

	"github.com/stretchr/testify/assert"
	"github.com/odpf/meteor/plugins"
	"google.golang.org/protobuf/types/known/anypb"
	g "github.com/odpf/meteor/plugins/sinks/gcs"
	testUtils "github.com/odpf/meteor/test/utils"
	"github.com/odpf/meteor/utils"
	"github.com/stretchr/testify/mock"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/models"
)

var validConfig = map[string]interface{}{
	"project_id":"google-project-id",
	"path":"bucket_name/target_folder",
	"object_prefix": "github-users",
	"service_account_base64" : "base 64 encoded key",
}

func TestInit(t *testing.T){

	t.Run("should return error if config is invalid",func(t *testing.T){
		gcsSink := g.New(testUtils.Logger)
		actualError := gcsSink.Init(context.TODO(), plugins.Config{RawConfig: map[string]interface{}{
			"project_id":"",
		}})
		assert.ErrorAs(t, actualError, &plugins.InvalidConfigError{})
	})

	t.Run("should retun error if service account json and service account base64 missing", func(t *testing.T){
		
		gcsSink := g.New(testUtils.Logger)
		actualError := gcsSink.Init(context.TODO(), plugins.Config{RawConfig: map[string]interface{}{
			"project_id":   "google-project-id",
			"path":         "bucket_name/target_folder",
		}})
		assert.ErrorContains(t, actualError, "credentials are not specified, failed to create client")
	})

	t.Run("should retun error if unable to decode base64 service account key", func(t *testing.T){
		
		gcsSink := g.New(testUtils.Logger)
		actualError := gcsSink.Init(context.TODO(), plugins.Config{RawConfig: map[string]interface{}{
			"project_id":              "google-project-id",
			"path":                    "bucket_name/target_folder",
			"service_account_base64":  "----", // invalid
		}})
		assert.ErrorContains(t, actualError, "failed to decode base64 service account")
	})
}

func TestSink(t *testing.T){

	t.Run("should write data in bucket and return nil error on success", func(t *testing.T){
		u := &v1beta2.User{
			FullName: "John Doe",
			Email:    "john.doe@odpf.com",
			Attributes: utils.TryParseMapToProto(map[string]interface{}{
				"org_unit_path": "/",
				"aliases":       "doe.john@odpf.com,johndoe@odpf.com",
			}),
		}
		user, _ := anypb.New(u)
		data := &v1beta2.Asset{
			Data: user,
		}
		jsonBytes, _ := models.ToJSON(data)

		ctx := context.TODO()
		client := new(mockClient)
		client.On("WriteData", jsonBytes).Return(nil)
		client.On("WriteData", []byte("\n")).Return(nil)

		gcsSink := g.New(testUtils.Logger)

		err := gcsSink.Init(context.TODO(), plugins.Config{RawConfig: map[string]interface{}{
			"project_id":              "google-project-id",
			"path":                    "bucket_name/target_folder",
			"service_account_json":  `{"type": "service_account"}`, 
		}})
		if err != nil {
			t.Fatal(err)
		}

		err = gcsSink.Sink(ctx,[]models.Record{models.NewRecord(data)})
		
		assert.NoError(t,err)
	})
}

type mockClient struct{
	mock.Mock
}

func (m *mockClient) WriteData(jsonBytes []byte) error{
	args := m.Called(jsonBytes)

	return args.Error(0)
} 

func (m *mockClient) Close() error{
	args := m.Called()

	return args.Error(0)
}