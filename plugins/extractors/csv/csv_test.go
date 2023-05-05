//go:build plugins
// +build plugins

package csv_test

import (
	"context"
	"testing"

	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/extractors/csv"
	"github.com/goto/meteor/test/mocks"
	"github.com/goto/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
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
			Attributes: &structpb.Struct{},
		})
		if err != nil {
			t.Fatal("error creating Any struct for test: %w", err)
		}
		expected := []*v1beta2.Asset{{
			Urn:     "urn:csv:test-csv:file:test.csv",
			Name:    "test.csv",
			Service: "csv",
			Type:    "table",
			Data:    table,
		}}

		utils.AssertEqualProtos(t, expected, emitter.GetAllData())
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
			Attributes: &structpb.Struct{},
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
			Attributes: &structpb.Struct{},
		})
		if err != nil {
			t.Fatal("error creating Any struct for test: %w", err)
		}
		expected := []*v1beta2.Asset{
			{
				Urn:     "urn:csv:test-csv:file:test-2.csv",
				Name:    "test-2.csv",
				Service: "csv",
				Type:    "table",
				Data:    table1,
			},
			{
				Urn:     "urn:csv:test-csv:file:test.csv",
				Name:    "test.csv",
				Service: "csv",
				Type:    "table",
				Data:    table2,
			},
		}
		utils.AssertEqualProtos(t, expected, emitter.GetAllData())
	})
}
