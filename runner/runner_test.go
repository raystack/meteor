package runner_test

import (
	"testing"

	"github.com/odpf/meteor/extractors"
	"github.com/odpf/meteor/processors"
	"github.com/odpf/meteor/recipes"
	"github.com/odpf/meteor/runner"
	"github.com/odpf/meteor/sinks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestRunnerRun(t *testing.T) {
	recipe := recipes.Recipe{
		Name: "sample",
		Source: recipes.Source{
			Type: "test-extractor",
		},
		Processors: []recipes.Processor{
			{Name: "test-processor", Config: map[string]interface{}{
				"proc-foo": "proc-bar",
			}},
		},
		Sinks: []recipes.Sink{
			{Name: "mock-sink", Config: map[string]interface{}{
				"url": "http://localhost:3000/data",
			}},
		},
	}

	t.Run("should return Run on finish", func(t *testing.T) {
		extr := new(testExtractor)
		proc := new(testProcessor)
		sink := new(mockSink)
		sink.On("Sink", mock.Anything, mock.Anything).Return(nil)

		extrStore := extractors.NewStore()
		extrStore.Populate(map[string]extractors.Extractor{
			"test-extractor": extr,
		})
		procStore := processors.NewStore()
		procStore.Populate(map[string]processors.Processor{
			"test-processor": proc,
		})
		sinkStore := sinks.NewStore()
		sinkStore.Populate(map[string]sinks.Sink{
			"mock-sink": sink,
		})

		expectedRun := &runner.Run{
			Recipe: recipe,
			Tasks: []runner.Task{
				{
					Type:   runner.TaskTypeExtract,
					Name:   recipe.Source.Type,
					Config: recipe.Source.Config,
					Status: runner.TaskStatusComplete,
				},
				{
					Type:   runner.TaskTypeProcess,
					Name:   recipe.Processors[0].Name,
					Config: recipe.Processors[0].Config,
					Status: runner.TaskStatusComplete,
				},
				{
					Type:   runner.TaskTypeSink,
					Name:   recipe.Sinks[0].Name,
					Config: recipe.Sinks[0].Config,
					Status: runner.TaskStatusComplete,
				},
			},
		}

		r := runner.New(extrStore, procStore, sinkStore)
		actual, err := r.Run(recipe)
		if err != nil {
			t.Error(err.Error())
		}

		assert.Equal(t, expectedRun.Recipe, actual.Recipe)
		assert.Equal(t, expectedRun.Tasks, actual.Tasks)
	})

	t.Run("should run extractor, processors and sinks", func(t *testing.T) {
		finalData := []map[string]interface{}{
			{
				"foo":  "bar",
				"test": true,
			},
			{
				"bar":  "foo",
				"test": true,
			},
		}

		extr := new(testExtractor)
		proc := new(testProcessor)
		sink := new(mockSink)
		sink.On("Sink", finalData, recipe.Sinks[0].Config).Return(nil)
		defer sink.AssertExpectations(t)

		extrStore := extractors.NewStore()
		extrStore.Populate(map[string]extractors.Extractor{
			"test-extractor": extr,
		})
		procStore := processors.NewStore()
		procStore.Populate(map[string]processors.Processor{
			"test-processor": proc,
		})
		sinkStore := sinks.NewStore()
		sinkStore.Populate(map[string]sinks.Sink{
			"mock-sink": sink,
		})

		runner := runner.New(extrStore, procStore, sinkStore)
		run, err := runner.Run(recipe)
		if err != nil {
			t.Error(err.Error())
		}

		assert.Equal(t, finalData, run.Data)
	})
}

type testProcessor struct{}

func (t *testProcessor) Process(data []map[string]interface{}, config map[string]interface{}) ([]map[string]interface{}, error) {
	for _, d := range data {
		d["test"] = true
	}

	return data, nil
}

type testExtractor struct{}

func (t *testExtractor) Extract(config map[string]interface{}) ([]map[string]interface{}, error) {
	data := []map[string]interface{}{
		{"foo": "bar"},
		{"bar": "foo"},
	}
	return data, nil
}

type mockSink struct {
	mock.Mock
}

func (m *mockSink) Sink(data []map[string]interface{}, config map[string]interface{}) error {
	args := m.Called(data, config)
	return args.Error(0)
}
