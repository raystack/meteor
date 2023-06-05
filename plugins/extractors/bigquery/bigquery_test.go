//go:build plugins
// +build plugins

package bigquery_test

import (
	"context"
	"testing"

	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/extractors/bigquery"
	"github.com/goto/meteor/test/utils"
	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	t.Run("should return error if config is invalid", func(t *testing.T) {
		extr := bigquery.New(utils.Logger)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := extr.Init(ctx, plugins.Config{
			URNScope: "test-bigquery",
			RawConfig: map[string]interface{}{
				"wrong-config": "sample-project",
			},
		})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
	t.Run("should not return invalid config error if config is valid", func(t *testing.T) {
		extr := bigquery.New(utils.Logger)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := extr.Init(ctx, plugins.Config{
			URNScope: "test-bigquery",
			RawConfig: map[string]interface{}{
				"project_id": "sample-project",
			},
		})

		assert.NotEqual(t, plugins.InvalidConfigError{}, err)
	})
	t.Run("should return error if service_account_base64 config is invalid", func(t *testing.T) {
		extr := bigquery.New(utils.Logger)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := extr.Init(ctx, plugins.Config{
			URNScope: "test-bigquery",
			RawConfig: map[string]interface{}{
				"project_id":             "google-project-id",
				"service_account_base64": "----", // invalid
			},
		})

		assert.ErrorContains(t, err, "decode base64 service account")
	})
}

func TestIsExcludedTable(t *testing.T) {
	type args struct {
		datasetID      string
		tableID        string
		excludedTables []string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "should return false when excluded table list is nil",
			args: args{
				datasetID:      "dataset_a",
				tableID:        "table_b",
				excludedTables: nil,
			},
			want: false,
		},
		{
			name: "should return false when excluded table list is empty",
			args: args{
				datasetID:      "dataset_a",
				tableID:        "table_b",
				excludedTables: []string{},
			},
			want: false,
		},
		{
			name: "should return false if table is not in excluded list",
			args: args{
				datasetID:      "dataset_a",
				tableID:        "table_b",
				excludedTables: []string{"ds1.table1", "playground.test_weekly"},
			},
			want: false,
		},
		{
			name: "should return true if table is in excluded list",
			args: args{
				datasetID:      "dataset_a",
				tableID:        "table_b",
				excludedTables: []string{"ds1.table1", "playground.test_weekly", "dataset_a.table_b"},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, bigquery.IsExcludedTable(tt.args.datasetID, tt.args.tableID, tt.args.excludedTables), "IsExcludedTable(%v, %v, %v)", tt.args.datasetID, tt.args.tableID, tt.args.excludedTables)
		})
	}
}

func TestIsExcludedDataset(t *testing.T) {
	type args struct {
		datasetID        string
		excludedDatasets []string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "should return false is list is empty",
			args: args{
				datasetID:        "dataset_a",
				excludedDatasets: []string{},
			},
			want: false,
		},
		{
			name: "should return false is list is nil",
			args: args{
				datasetID:        "dataset_a",
				excludedDatasets: nil,
			},
			want: false,
		},
		{
			name: "should return false is dataset is not in excluded list",
			args: args{
				datasetID:        "dataset_a",
				excludedDatasets: []string{"dataset_b", "dataset_c"},
			},
			want: false,
		},
		{
			name: "should return true is dataset is in excluded list",
			args: args{
				datasetID:        "dataset_a",
				excludedDatasets: []string{"dataset_a", "dataset_b", "dataset_c"},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, bigquery.IsExcludedDataset(tt.args.datasetID, tt.args.excludedDatasets), "IsExcludedDataset(%v, %v)", tt.args.datasetID, tt.args.excludedDatasets)
		})
	}
}
