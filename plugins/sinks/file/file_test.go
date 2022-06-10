package file_test

import (
	"context"
	"testing"

	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	f "github.com/odpf/meteor/plugins/sinks/file"
	"github.com/stretchr/testify/assert"
)

var validConfig = map[string]interface{}{
	"path": "sample.json",
}

func TestInit(t *testing.T) {
	t.Run("should return InvalidConfigError on invalid config", func(t *testing.T) {
		invalidConfig := map[string]interface{}{}
		fileSink := f.New()
		err := fileSink.Init(context.TODO(), invalidConfig)
		assert.Equal(t, plugins.InvalidConfigError{Type: "sink", PluginName: "file"}, err)
	})
	t.Run("should return error on filename missing", func(t *testing.T) {
		invalidConfig := map[string]interface{}{
			"path": "./some-dir",
		}
		fileSink := f.New()
		err := fileSink.Init(context.TODO(), invalidConfig)
		assert.Error(t, err)
	})
	t.Run("should return error on invalid file format extension", func(t *testing.T) {
		invalidConfig := map[string]interface{}{
			"path": "./sample.txt",
		}
		fileSink := f.New()
		err := fileSink.Init(context.TODO(), invalidConfig)
		assert.Error(t, err)
	})
	t.Run("should return no error on valid config", func(t *testing.T) {
		fileSink := f.New()
		err := fileSink.Init(context.TODO(), validConfig)
		assert.NoError(t, err)
	})
}

func TestMain(t *testing.T) {
	t.Run("sink test", func(t *testing.T) {
		fileSink := f.New()
		err := fileSink.Init(context.TODO(), validConfig)
		assert.NoError(t, err)
		err = fileSink.Sink(context.TODO(), getExpectedVal())
		assert.NoError(t, err)
	})
	t.Run("sink test", func(t *testing.T) {
		yamlConf := map[string]interface{}{
			"path": "sample.yaml",
		}
		fileSink := f.New()
		err := fileSink.Init(context.TODO(), yamlConf)
		assert.NoError(t, err)
		err = fileSink.Sink(context.TODO(), getExpectedVal())
		assert.NoError(t, err)
	})
}

func getExpectedVal() []models.Record {
	return []models.Record{
		models.NewRecord(&assetsv1beta1.Table{
			Resource: &commonv1beta1.Resource{
				Urn:  "elasticsearch.index1",
				Name: "index1",
				Type: "table",
			},
			Schema: &facetsv1beta1.Columns{
				Columns: []*facetsv1beta1.Column{
					{
						Name:     "SomeInt",
						DataType: "long",
					},
					{
						Name:     "SomeStr",
						DataType: "text",
					},
				},
			},
			Profile: &assetsv1beta1.TableProfile{
				TotalRows: 1,
			},
		}),
		models.NewRecord(&assetsv1beta1.Table{
			Resource: &commonv1beta1.Resource{
				Urn:  "elasticsearch.index2",
				Name: "index2",
				Type: "table",
			},
			Schema: &facetsv1beta1.Columns{
				Columns: []*facetsv1beta1.Column{
					{
						Name:     "SomeInt",
						DataType: "long",
					},
					{
						Name:     "SomeStr",
						DataType: "text",
					},
				},
			},
			Profile: &assetsv1beta1.TableProfile{
				TotalRows: 1,
			},
		}),
	}
}
