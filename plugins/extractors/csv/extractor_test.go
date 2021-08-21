//+build integration

package csv_test

import (
	"context"
	"errors"
	"testing"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/csv"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/proto/odpf/meta/facets"
	logger "github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
)

var lg = logger.NewLogrus()

func TestExtract(t *testing.T) {
	t.Run("should return error if fileName and directory both are empty", func(t *testing.T) {
		config := map[string]interface{}{}
		err := csv.New(lg).Extract(
			context.TODO(),
			config,
			make(chan<- interface{}))
		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})

	t.Run("should extract data if path is a file", func(t *testing.T) {
		config := map[string]interface{}{
			"path": "./testdata/test.csv",
		}
		out := make(chan interface{})
		go func() {
			err := csv.New(lg).Extract(
				context.TODO(),
				config,
				out)
			close(out)
			assert.NoError(t, err)
		}()

		var results []meta.Table
		for d := range out {
			table, ok := d.(meta.Table)
			if !ok {
				t.Fatal(errors.New("invalid table format"))
			}

			results = append(results, table)
		}

		expected := []meta.Table{
			{
				Urn:    "test.csv",
				Name:   "test.csv",
				Source: "csv",
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
		out := make(chan interface{})
		go func() {
			err := csv.New(lg).Extract(
				context.TODO(),
				config,
				out)
			close(out)
			assert.NoError(t, err)
		}()

		var results []meta.Table
		for d := range out {
			table, ok := d.(meta.Table)
			if !ok {
				t.Fatal(errors.New("invalid table format"))
			}

			results = append(results, table)
		}

		expected := []meta.Table{
			{
				Urn:    "test-2.csv",
				Name:   "test-2.csv",
				Source: "csv",
				Schema: &facets.Columns{
					Columns: []*facets.Column{
						{Name: "order"},
						{Name: "transaction_id"},
						{Name: "total_price"},
					},
				},
			},
			{
				Urn:    "test.csv",
				Name:   "test.csv",
				Source: "csv",
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
