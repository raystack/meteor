//+build integration

package csv_test

import (
	"context"
	"errors"
	"testing"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/models/odpf/assets/facets"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/csv"
	"github.com/odpf/meteor/test"
	"github.com/stretchr/testify/assert"
)

func TestExtract(t *testing.T) {
	t.Run("should return error if fileName and directory both are empty", func(t *testing.T) {
		config := map[string]interface{}{}
		err := csv.New(test.Logger).Extract(
			context.TODO(),
			config,
			make(chan<- models.Record))
		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})

	t.Run("should extract data if path is a file", func(t *testing.T) {
		config := map[string]interface{}{
			"path": "./testdata/test.csv",
		}
		out := make(chan models.Record)
		go func() {
			err := csv.New(test.Logger).Extract(
				context.TODO(),
				config,
				out)
			close(out)
			assert.NoError(t, err)
		}()

		var results []*assets.Table
		for d := range out {
			table, ok := d.Data().(*assets.Table)
			if !ok {
				t.Fatal(errors.New("invalid table format"))
			}

			results = append(results, table)
		}

		expected := []*assets.Table{
			{
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
			},
		}
		assert.Equal(t, expected, results)
	})

	t.Run("should extract data from all files if path is a dir", func(t *testing.T) {
		config := map[string]interface{}{
			"path": "./testdata",
		}
		out := make(chan models.Record)
		go func() {
			err := csv.New(test.Logger).Extract(
				context.TODO(),
				config,
				out)
			close(out)
			assert.NoError(t, err)
		}()

		var results []*assets.Table
		for d := range out {
			table, ok := d.Data().(*assets.Table)
			if !ok {
				t.Fatal(errors.New("invalid table format"))
			}

			results = append(results, table)
		}

		expected := []*assets.Table{
			{
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
			},
			{
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
			},
		}
		assert.Equal(t, expected, results)
	})
}
