//go:build plugins
// +build plugins

package csv_test

import (
	"context"
	"testing"

	"github.com/goto/meteor/test/utils"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/extractors/csv"
	"github.com/goto/meteor/test/mocks"
	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	t.Run("should return error if fileName and directory both are empty", func(t *testing.T) {
		config := map[string]interface{}{}
		err := csv.New(utils.Logger).Init(
			context.TODO(),
			plugins.Config{
				URNScope:  "test-csv",
				RawConfig: config,
			})
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
}

func TestExtract(t *testing.T) {
	t.Run("should extract data if path is a file", func(t *testing.T) {
		ctx := context.TODO()
		extr := csv.New(utils.Logger)
		err := extr.Init(ctx, plugins.Config{
			URNScope: "test-csv",
			RawConfig: map[string]interface{}{
				"path": "./testdata/test.csv",
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)
		assert.NoError(t, err)
		table, err := anypb.New(&v1beta2.Table{
			Columns: []*v1beta2.Column{
				{Name: "name"},
				{Name: "age"},
				{Name: "phone"},
			},
		})
		if err != nil {
			t.Fatal("error creating Any struct for test: %w", err)
		}
		expected := []models.Record{
			models.NewRecord(&v1beta2.Asset{
				Urn:     "urn:csv:test-csv:file:test.csv",
				Name:    "test.csv",
				Service: "csv",
				Type:    "table",
				Data:    table,
			}),
		}

		assert.Equal(t, expected, emitter.Get())
	})

	t.Run("should extract data from all files if path is a dir", func(t *testing.T) {
		ctx := context.TODO()
		extr := csv.New(utils.Logger)
		err := extr.Init(ctx, plugins.Config{
			URNScope: "test-csv",
			RawConfig: map[string]interface{}{
				"path": "./testdata",
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)
		assert.NoError(t, err)
		table1, err := anypb.New(&v1beta2.Table{
			Columns: []*v1beta2.Column{
				{Name: "order"},
				{Name: "transaction_id"},
				{Name: "total_price"},
			},
		})
		if err != nil {
			t.Fatal("error creating Any struct for test: %w", err)
		}
		table2, err := anypb.New(&v1beta2.Table{
			Columns: []*v1beta2.Column{
				{Name: "name"},
				{Name: "age"},
				{Name: "phone"},
			},
		})
		if err != nil {
			t.Fatal("error creating Any struct for test: %w", err)
		}
		expected := []models.Record{
			models.NewRecord(&v1beta2.Asset{
				Urn:     "urn:csv:test-csv:file:test-2.csv",
				Name:    "test-2.csv",
				Service: "csv",
				Type:    "table",
				Data:    table1,
			}),
			models.NewRecord(&v1beta2.Asset{
				Urn:     "urn:csv:test-csv:file:test.csv",
				Name:    "test.csv",
				Service: "csv",
				Type:    "table",
				Data:    table2,
			}),
		}
		assert.Equal(t, expected, emitter.Get())
	})
}
