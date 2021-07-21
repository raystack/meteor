package extractor_test

import (
	"testing"

	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	t.Run("should return new extractor instance with given factory", func(t *testing.T) {
		factory := extractor.NewFactory()
		extr := extractor.New(factory)

		assert.NotNil(t, extr)
	})
}

func TestExtractorExtract(t *testing.T) {
	t.Run("should return NotFoundError if extractor cannot be found", func(t *testing.T) {
		extractorName := "wrong-extr"
		factory := extractor.NewFactory()
		extr := extractor.New(factory)

		_, err := extr.Extract(extractorName, map[string]interface{}{})

		assert.Equal(t, extractor.NotFoundError{Name: extractorName}, err)
	})

	t.Run("should return data from extractor", func(t *testing.T) {
		extractorName := "test-extr"
		data := []meta.Table{
			{
				Urn: "foo-1",
			},
			{
				Urn: "foo-2",
			},
		}
		tableExtractor := &tableExtractor{
			data: data,
		}
		factory := extractor.NewFactory()
		factory.SetTableExtractor(extractorName, func() extractor.TableExtractor {
			return tableExtractor
		})
		extr := extractor.New(factory)

		result, err := extr.Extract(extractorName, map[string]interface{}{})
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, data, result)
	})
}

type tableExtractor struct {
	data []meta.Table
}

func (e *tableExtractor) Extract(config map[string]interface{}) ([]meta.Table, error) {
	return e.data, nil
}
