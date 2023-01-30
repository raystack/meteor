//go:build plugins
// +build plugins
package gcs_test

import(
	"testing"
	"context"

	"github.com/stretchr/testify/assert"
	"github.com/odpf/meteor/plugins"
	g "github.com/odpf/meteor/plugins/sinks/gcs"
	testUtils "github.com/odpf/meteor/test/utils"
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
		assert.ErrorContains(t, actualError, "failed to create client")
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