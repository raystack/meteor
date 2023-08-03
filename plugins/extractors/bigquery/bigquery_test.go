//go:build plugins
// +build plugins

package bigquery_test

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/extractors/bigquery"
	"github.com/goto/meteor/test/mocks"
	"github.com/goto/meteor/test/utils"
	slog "github.com/goto/salt/log"
	"github.com/nsf/jsondiff"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	projectID = "test-project-id"
)

var client *bq.Client

func TestMain(m *testing.M) {
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	// setup test
	opts := dockertest.RunOptions{
		Repository: "ghcr.io/goccy/bigquery-emulator",
		Tag:        "0.3",
		Env:        []string{},
		Mounts: []string{
			fmt.Sprintf("%s/testdata:/work/testdata", pwd),
		},
		Cmd: []string{
			"--project=" + projectID,
			"--data-from-yaml=/work/testdata/data.yaml",
		},
		ExposedPorts: []string{"9050"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9050": {
				{HostIP: "0.0.0.0", HostPort: "9050"},
			},
		},
	}
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	retryFn := func(resource *dockertest.Resource) error {
		if client, err = bq.NewClient(context.Background(), projectID,
			option.WithEndpoint("http://localhost:9050"),
			option.WithoutAuthentication(),
		); err != nil {
			return err
		}

		// Perform a simple query to check connectivity.
		if _, err = client.Query("SELECT 1").Run(context.Background()); err != nil {
			return err
		}

		return nil
	}
	purgeFn, err := utils.CreateContainer(opts, retryFn)
	if err != nil {
		log.Fatal(err)
	}

	// run tests
	code := m.Run()

	// clean tests
	client.Close()
	if err := purgeFn(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

func mockClient(ctx context.Context, logger slog.Logger, config *bigquery.Config) (*bq.Client, error) {
	return client, nil
}

func TestInit(t *testing.T) {
	t.Run("should return error if config is invalid", func(t *testing.T) {
		extr := bigquery.New(utils.Logger, bigquery.CreateClient, nil)
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
		extr := bigquery.New(utils.Logger, bigquery.CreateClient, nil)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := extr.Init(ctx, plugins.Config{
			URNScope: "test-bigquery",
			RawConfig: map[string]interface{}{
				"project_id":          "sample-project",
				"collect_table_usage": true,
			},
		})

		assert.NotEqual(t, plugins.InvalidConfigError{}, err)
	})
	t.Run("should return error if service_account_base64 config is invalid", func(t *testing.T) {
		extr := bigquery.New(utils.Logger, bigquery.CreateClient, nil)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := extr.Init(ctx, plugins.Config{
			URNScope: "test-bigquery",
			RawConfig: map[string]interface{}{
				"project_id":             projectID,
				"service_account_base64": "----", // invalid
			},
		})

		assert.ErrorContains(t, err, "decode base64 service account")
	})

	t.Run("should return no error", func(t *testing.T) {
		extr := bigquery.New(utils.Logger, mockClient, nil)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := extr.Init(ctx, plugins.Config{
			URNScope: "test-bigquery",
			RawConfig: map[string]interface{}{
				"project_id": projectID,
			},
		})

		assert.NoError(t, err)
	})
}

func TestExtract(t *testing.T) {
	runTest := func(t *testing.T, cfg plugins.Config, randomizer func(seed int64) func(int64) int64) []*v1beta2.Asset {
		extr := bigquery.New(utils.Logger, mockClient, randomizer)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		err := extr.Init(ctx, cfg)

		assert.NoError(t, err)

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)
		assert.NoError(t, err)

		actual := getAllData(emitter, t)
		return actual
	}

	t.Run("should return no error", func(t *testing.T) {
		actual := runTest(t, plugins.Config{
			URNScope: "test-bigquery",
			RawConfig: map[string]interface{}{
				"project_id":             projectID,
				"max_preview_rows":       "1",
				"include_column_profile": "true",
				"exclude": map[string]interface{}{
					"datasets": []string{"exclude_this_dataset"},
					"tables":   []string{"dataset1.exclude_this_table"},
				},
			},
		}, nil)

		utils.AssertProtosWithJSONFile(t, "testdata/expected-assets.json", actual)
	})

	t.Run("with mix_values true", func(t *testing.T) {
		cfg := plugins.Config{
			URNScope: "test-bigquery",
			RawConfig: map[string]interface{}{
				"project_id":             projectID,
				"max_preview_rows":       "5",
				"mix_values":             "true",
				"include_column_profile": "true",
				"exclude": map[string]interface{}{
					"datasets": []string{"exclude_this_dataset"},
					"tables":   []string{"dataset1.exclude_this_table"},
				},
			},
		}

		randFn := func(mainSeed int64) func(seed int64) func(max int64) int64 {
			r := rand.New(rand.NewSource(mainSeed))
			return func(seed int64) func(max int64) int64 {
				return func(max int64) int64 {
					return r.Int63n(max)
				}
			}
		}

		t.Run("should return preview rows with mixed values", func(t *testing.T) {
			actual := runTest(t, cfg, randFn(1))

			utils.AssertJSONFile(t, "testdata/expected-assets-mixed.json", actual, jsondiff.FullMatch)
		})

		t.Run("with different seed should not equal to expected", func(t *testing.T) {
			actual := runTest(t, cfg, randFn(2))
			utils.AssertJSONFile(t, "testdata/expected-assets-mixed.json", actual, jsondiff.NoMatch)
		})

		t.Run("should not randomize if rows < 2", func(t *testing.T) {
			newCfg := cfg
			newCfg.RawConfig["max_preview_rows"] = "1"

			actual := runTest(t, newCfg, randFn(1))
			utils.AssertJSONFile(t, "testdata/expected-assets.json", actual, jsondiff.FullMatch)
		})
	})
}

func getAllData(emitter *mocks.Emitter, t *testing.T) []*v1beta2.Asset {
	actual := emitter.GetAllData()

	// the emulator appending 1 random dataset
	// we can't assert it, so we remove it from the list
	actual = actual[:len(actual)-1]

	// the emulator returning dynamic timestamps
	// replace them with static ones
	for _, asset := range actual {
		replaceWithStaticTimestamp(t, asset)
	}
	return actual
}

func replaceWithStaticTimestamp(t *testing.T, asset *v1beta2.Asset) {
	b := new(v1beta2.Table)
	err := asset.Data.UnmarshalTo(b)
	assert.NoError(t, err)

	time, err := time.Parse(time.RFC3339, "2023-06-13T03:46:12.372974Z")
	assert.NoError(t, err)
	b.CreateTime = timestamppb.New(time)
	b.UpdateTime = timestamppb.New(time)

	asset.Data, err = anypb.New(b)
	assert.NoError(t, err)
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
