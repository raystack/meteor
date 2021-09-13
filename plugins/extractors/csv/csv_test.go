//+build integration

package csv_test

import (
	"context"
	"testing"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/models/odpf/assets/facets"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/csv"
	"github.com/odpf/meteor/test"
	"github.com/odpf/meteor/test/mocks"
	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	t.Run("should return error if fileName and directory both are empty", func(t *testing.T) {
		config := map[string]interface{}{}
		err := csv.New(test.Logger).Init(
			context.TODO(),
			config)
		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})
}

func TestExtract(t *testing.T) {
	t.Run("should extract data if path is a file", func(t *testing.T) {
		ctx := context.TODO()
		extr := csv.New(test.Logger)
		err := extr.Init(ctx, map[string]interface{}{
			"path": "./testdata/test.csv",
		})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter)
		assert.NoError(t, err)

		expected := []models.Record{
			models.NewRecord(&assets.Table{
				Resource: &common.Resource{
					Urn:     "test.csv",
					Name:    "test.csv",
					Service: "csv",
				},
				Schema: &facets.Columns{
					Columns: []*facets.Column{
						{Name: "name"},
						{Name: "age"},
						{Name: "phone"},
					},
				},
			}),
		}

		assert.Equal(t, expected, emitter.Get())
	})

	t.Run("should extract data from all files if path is a dir", func(t *testing.T) {
		ctx := context.TODO()
		extr := csv.New(test.Logger)
		err := extr.Init(ctx, map[string]interface{}{
			"path": "./testdata",
		})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter)
		assert.NoError(t, err)

		expected := []models.Record{
			models.NewRecord(&assets.Table{
				Resource: &common.Resource{
					Urn:     "test-2.csv",
					Name:    "test-2.csv",
					Service: "csv",
				},
				Schema: &facets.Columns{
					Columns: []*facets.Column{
						{Name: "order"},
						{Name: "transaction_id"},
						{Name: "total_price"},
					},
				},
			}),
			models.NewRecord(&assets.Table{
				Resource: &common.Resource{
					Urn:     "test.csv",
					Name:    "test.csv",
					Service: "csv",
				},
				Schema: &facets.Columns{
					Columns: []*facets.Column{
						{Name: "name"},
						{Name: "age"},
						{Name: "phone"},
					},
				},
			}),
		}
		assert.Equal(t, expected, emitter.Get())
	})
}
