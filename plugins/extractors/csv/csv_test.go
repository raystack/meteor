//go:build plugins
// +build plugins

package csv_test

import (
	"context"
	"testing"

	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/extractors/csv"
	"github.com/raystack/meteor/test/mocks"
	"github.com/raystack/meteor/test/utils"
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

		expected := []*meteorv1beta1.Entity{
			models.NewEntity("urn:csv:test-csv:file:test.csv", "table", "test.csv", "csv", map[string]interface{}{
				"columns": []interface{}{
					map[string]interface{}{"name": "name"},
					map[string]interface{}{"name": "age"},
					map[string]interface{}{"name": "phone"},
				},
			}),
		}

		utils.AssertEqualProtos(t, expected, emitter.GetAllEntities())
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

		expected := []*meteorv1beta1.Entity{
			models.NewEntity("urn:csv:test-csv:file:test-2.csv", "table", "test-2.csv", "csv", map[string]interface{}{
				"columns": []interface{}{
					map[string]interface{}{"name": "order"},
					map[string]interface{}{"name": "transaction_id"},
					map[string]interface{}{"name": "total_price"},
				},
			}),
			models.NewEntity("urn:csv:test-csv:file:test.csv", "table", "test.csv", "csv", map[string]interface{}{
				"columns": []interface{}{
					map[string]interface{}{"name": "name"},
					map[string]interface{}{"name": "age"},
					map[string]interface{}{"name": "phone"},
				},
			}),
		}
		utils.AssertEqualProtos(t, expected, emitter.GetAllEntities())
	})
}
