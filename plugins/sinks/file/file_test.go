//go:build plugins
// +build plugins

package file_test

import (
	"context"
	_ "embed"
	"testing"

	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	f "github.com/raystack/meteor/plugins/sinks/file"
	testUtils "github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/structpb"
)

var validConfig = map[string]interface{}{
	"path":   "./test-dir/sample.ndjson",
	"format": "ndjson",
}

func TestValidate(t *testing.T) {
	t.Run("should return error on invalid config", func(t *testing.T) {
		invalidConfig := map[string]interface{}{}
		fileSink := f.New(testUtils.Logger)
		err := fileSink.Validate(plugins.Config{RawConfig: invalidConfig})
		assert.Error(t, err)
	})
}

func TestInit(t *testing.T) {
	t.Run("should return InvalidConfigError on invalid config", func(t *testing.T) {
		invalidConfig := map[string]interface{}{}
		fileSink := f.New(testUtils.Logger)
		err := fileSink.Init(context.TODO(), plugins.Config{RawConfig: invalidConfig})
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
	t.Run("should return error if file is not found ", func(t *testing.T) {
		invalidConfig := map[string]interface{}{
			"path":   "./some-dir",
			"format": "ndjson",
		}
		fileSink := f.New(testUtils.Logger)
		err := fileSink.Init(context.TODO(), plugins.Config{RawConfig: invalidConfig})
		assert.Error(t, err)
	})
	t.Run("should return no error on valid config", func(t *testing.T) {
		fileSink := f.New(testUtils.Logger)
		err := fileSink.Init(context.TODO(), plugins.Config{RawConfig: validConfig})
		assert.NoError(t, err)
	})
}

func TestSink(t *testing.T) {
	t.Run("should return no error with for valid ndjson config", func(t *testing.T) {
		assert.NoError(t, sinkValidSetup(t, validConfig))
	})
	t.Run("should return no error with for valid yaml config", func(t *testing.T) {
		config := map[string]interface{}{
			"path":   "./test-dir/sample.yaml",
			"format": "yaml",
		}
		assert.NoError(t, sinkValidSetup(t, config))

	})
	t.Run("should return error for invalid directory in yaml", func(t *testing.T) {
		config := map[string]interface{}{
			"path":   "./test-dir/some-dir/sample.yaml",
			"format": "yaml",
		}
		assert.Error(t, sinkInvalidPath(t, config))
	})
	t.Run("should return error for invalid directory in ndjson", func(t *testing.T) {
		config := map[string]interface{}{
			"path":   "./test-dir/some-dir/sample.ndjson",
			"format": "ndjson",
		}
		assert.Error(t, sinkInvalidPath(t, config))
	})
}

func sinkInvalidPath(t *testing.T, config map[string]interface{}) error {
	t.Helper()

	fileSink := f.New(testUtils.Logger)
	return fileSink.Init(context.TODO(), plugins.Config{RawConfig: config})
}

func sinkValidSetup(t *testing.T, config map[string]interface{}) error {
	t.Helper()

	fileSink := f.New(testUtils.Logger)
	err := fileSink.Init(context.TODO(), plugins.Config{RawConfig: config})
	assert.NoError(t, err)
	err = fileSink.Sink(context.TODO(), getExpectedVal(t))
	assert.NoError(t, err)
	return fileSink.Close()
}

func getExpectedVal(t *testing.T) []models.Record {
	t.Helper()

	props1, err := structpb.NewStruct(map[string]interface{}{
		"columns": []interface{}{
			map[string]interface{}{
				"name":      "SomeStr",
				"data_type": "text",
			},
		},
		"profile": map[string]interface{}{
			"total_rows": float64(1),
		},
	})
	if err != nil {
		t.Fatal("error creating properties for test:", err)
	}
	props2, err := structpb.NewStruct(map[string]interface{}{
		"columns": []interface{}{
			map[string]interface{}{
				"name":      "SomeStr",
				"data_type": "text",
			},
		},
		"profile": map[string]interface{}{
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
