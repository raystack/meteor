package recipes_test

import (
	"testing"

	"github.com/odpf/meteor/extractors"
	"github.com/odpf/meteor/processors"
	"github.com/odpf/meteor/recipes"
	"github.com/odpf/meteor/sinks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var validRecipe = recipes.Recipe{
	Name: "sample",
	Source: recipes.SourceRecipe{
		Type: "test-extractor",
	},
	Processors: []recipes.ProcessorRecipe{
		{Name: "test-processor", Config: map[string]interface{}{
			"proc-foo": "proc-bar",
		}},
	},
	Sinks: []recipes.SinkRecipe{
		{Name: "mock-sink", Config: map[string]interface{}{
			"url": "http://localhost:3000/data",
		}},
	},
}

func TestRunnerRun(t *testing.T) {
	recipe := validRecipe

	t.Run("should return error on failed run", func(t *testing.T) {
		extrFactory := extractors.NewFactory()
		procFactory := processors.NewFactory()
		sinkFactory := sinks.NewFactory()

		r := recipes.NewRunner(extrFactory, procFactory, sinkFactory, nil)
		_, err := r.Run(recipe)

		assert.NotNil(t, err)
	})

	t.Run("should return Run on finish", func(t *testing.T) {
		sink := new(mockSink)
		sink.On("Sink", mock.Anything, mock.Anything).Return(nil)

		extrFactory := extractors.NewFactory()
		extrFactory.Set("test-extractor", newTestExtractor)
		procFactory := processors.NewFactory()
		procFactory.Set("test-processor", newTestProcessor)
		sinkFactory := sinks.NewFactory()
		sinkFactory.Set("mock-sink", newTestSink(sink))

		expectedRun := &recipes.Run{
			Recipe: recipe,
			Tasks: []recipes.Task{
				{
					Type:   recipes.TaskTypeExtract,
					Name:   recipe.Source.Type,
					Config: recipe.Source.Config,
					Status: recipes.TaskStatusComplete,
				},
				{
					Type:   recipes.TaskTypeProcess,
					Name:   recipe.Processors[0].Name,
					Config: recipe.Processors[0].Config,
					Status: recipes.TaskStatusComplete,
				},
				{
					Type:   recipes.TaskTypeSink,
					Name:   recipe.Sinks[0].Name,
					Config: recipe.Sinks[0].Config,
					Status: recipes.TaskStatusComplete,
				},
			},
		}
		r := recipes.NewRunner(extrFactory, procFactory, sinkFactory, nil)
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

		sink := new(mockSink)
		sink.On("Sink", finalData, recipe.Sinks[0].Config).Return(nil)
		defer sink.AssertExpectations(t)

		extrFactory := extractors.NewFactory()
		extrFactory.Set("test-extractor", newTestExtractor)
		procFactory := processors.NewFactory()
		procFactory.Set("test-processor", newTestProcessor)
		sinkFactory := sinks.NewFactory()
		sinkFactory.Set("mock-sink", newTestSink(sink))

		r := recipes.NewRunner(extrFactory, procFactory, sinkFactory, nil)
		run, err := r.Run(recipe)
		if err != nil {
			t.Error(err.Error())
		}

		assert.Equal(t, finalData, run.Data)
	})

	t.Run("should record metrics", func(t *testing.T) {
		extrFactory := extractors.NewFactory()
		extrFactory.Set("test-extractor", newTestExtractor)
		procFactory := processors.NewFactory()
		procFactory.Set("test-processor", newTestProcessor)
		sink := new(mockSink)
		sink.On("Sink", mock.Anything, mock.Anything).Return(nil)
		sinkFactory := sinks.NewFactory()
		sinkFactory.Set("mock-sink", newTestSink(sink))
		monitor := new(mockMonitor)
		monitor.On("RecordRun", recipe, mock.AnythingOfType("int"), true).Once()
		defer monitor.AssertExpectations(t)

		r := recipes.NewRunner(extrFactory, procFactory, sinkFactory, monitor)
		_, err := r.Run(recipe)
		if err != nil {
			t.Error(err.Error())
		}
	})
}

func TestRunnerRunMultiple(t *testing.T) {
	validRecipe2 := validRecipe
	validRecipe2.Name = "sample-2"

	t.Run("should return list of failed recipes when finished", func(t *testing.T) {
		failedRecipe := recipes.Recipe{
			Name: "failedRecipe",
		}
		recipeList := []recipes.Recipe{validRecipe, failedRecipe, validRecipe2}

		extrFactory := extractors.NewFactory()
		extrFactory.Set("test-extractor", newTestExtractor)
		procFactory := processors.NewFactory()
		procFactory.Set("test-processor", newTestProcessor)
		sink := new(mockSink)
		sink.On("Sink", mock.Anything, mock.Anything).Return(nil)
		sinkFactory := sinks.NewFactory()
		sinkFactory.Set("mock-sink", newTestSink(sink))

		r := recipes.NewRunner(extrFactory, procFactory, sinkFactory, nil)
		faileds, err := r.RunMultiple(recipeList)
		assert.Nil(t, err)

		expected := []string{
			failedRecipe.Name,
		}
		assert.Equal(t, expected, faileds)
	})

	t.Run("should run all extractors, processors and sinks for all recipes", func(t *testing.T) {
		validRecipe2.Sinks = []recipes.SinkRecipe{
			{Name: "mock-sink-2"},
		}

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

		recipeList := []recipes.Recipe{validRecipe, validRecipe2}

		extrFactory := extractors.NewFactory()
		extrFactory.Set("test-extractor", newTestExtractor)
		procFactory := processors.NewFactory()
		procFactory.Set("test-processor", newTestProcessor)
		sink1 := new(mockSink)
		sink2 := new(mockSink)
		sink1.On("Sink", finalData, validRecipe.Sinks[0].Config).Return(nil).Once()
		defer sink1.AssertExpectations(t)
		sink2.On("Sink", finalData, validRecipe2.Sinks[0].Config).Return(nil).Once()
		defer sink2.AssertExpectations(t)
		sinkFactory := sinks.NewFactory()
		sinkFactory.Set("mock-sink", newTestSink(sink1))
		sinkFactory.Set("mock-sink-2", newTestSink(sink2))

		r := recipes.NewRunner(extrFactory, procFactory, sinkFactory, nil)
		faileds, err := r.RunMultiple(recipeList)
		assert.Nil(t, err)
		assert.Equal(t, []string{}, faileds)
	})
}

type testProcessor struct{}

func newTestProcessor() processors.Processor {
	return &testProcessor{}
}

func (t *testProcessor) Process(data []map[string]interface{}, config map[string]interface{}) ([]map[string]interface{}, error) {
	for _, d := range data {
		d["test"] = true
	}

	return data, nil
}

type testExtractor struct{}

func newTestExtractor() extractors.Extractor {
	return &testExtractor{}
}

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

func newTestSink(sink sinks.Sink) sinks.FactoryFn {
	return func() sinks.Sink {
		return sink
	}
}

func (m *mockSink) Sink(data []map[string]interface{}, config map[string]interface{}) error {
	args := m.Called(data, config)
	return args.Error(0)
}

type mockMonitor struct {
	mock.Mock
}

func (m *mockMonitor) RecordRun(recipe recipes.Recipe, durationInMs int, success bool) {
	m.Called(recipe, durationInMs, success)
}
