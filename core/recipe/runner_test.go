package recipe_test

import (
	"testing"

	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/core/processor"
	"github.com/odpf/meteor/core/recipe"
	"github.com/odpf/meteor/core/sink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var validRecipe = recipe.Recipe{
	Name: "sample",
	Source: recipe.SourceRecipe{
		Type: "test-extractor",
	},
	Processors: []recipe.ProcessorRecipe{
		{Name: "test-processor", Config: map[string]interface{}{
			"proc-foo": "proc-bar",
		}},
	},
	Sinks: []recipe.SinkRecipe{
		{Name: "mock-sink", Config: map[string]interface{}{
			"url": "http://localhost:3000/data",
		}},
	},
}

func TestRunnerRun(t *testing.T) {
	rcp := validRecipe

	t.Run("should return error on failed run", func(t *testing.T) {
		extrFactory := extractor.NewFactory()
		procFactory := processor.NewFactory()
		sinkFactory := sink.NewFactory()

		r := recipe.NewRunner(extrFactory, procFactory, sinkFactory, nil)
		_, err := r.Run(rcp)

		assert.NotNil(t, err)
	})

	t.Run("should return Run on finish", func(t *testing.T) {
		mSink := new(mockSink)
		mSink.On("Sink", mock.Anything, mock.Anything).Return(nil)

		extrFactory := extractor.NewFactory()
		extrFactory.Set("test-extractor", newTestExtractor)
		procFactory := processor.NewFactory()
		procFactory.Set("test-processor", newTestProcessor)
		sinkFactory := sink.NewFactory()
		sinkFactory.Set("mock-sink", newTestSink(mSink))

		expectedRun := &recipe.Run{
			Recipe: rcp,
			Tasks: []recipe.Task{
				{
					Type:   recipe.TaskTypeExtract,
					Name:   rcp.Source.Type,
					Config: rcp.Source.Config,
					Status: recipe.TaskStatusComplete,
				},
				{
					Type:   recipe.TaskTypeProcess,
					Name:   rcp.Processors[0].Name,
					Config: rcp.Processors[0].Config,
					Status: recipe.TaskStatusComplete,
				},
				{
					Type:   recipe.TaskTypeSink,
					Name:   rcp.Sinks[0].Name,
					Config: rcp.Sinks[0].Config,
					Status: recipe.TaskStatusComplete,
				},
			},
		}
		r := recipe.NewRunner(extrFactory, procFactory, sinkFactory, nil)
		actual, err := r.Run(rcp)
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

		mSink := new(mockSink)
		mSink.On("Sink", finalData, rcp.Sinks[0].Config).Return(nil)
		defer mSink.AssertExpectations(t)

		extrFactory := extractor.NewFactory()
		extrFactory.Set("test-extractor", newTestExtractor)
		procFactory := processor.NewFactory()
		procFactory.Set("test-processor", newTestProcessor)
		sinkFactory := sink.NewFactory()
		sinkFactory.Set("mock-sink", newTestSink(mSink))

		r := recipe.NewRunner(extrFactory, procFactory, sinkFactory, nil)
		run, err := r.Run(rcp)
		if err != nil {
			t.Error(err.Error())
		}

		assert.Equal(t, finalData, run.Data)
	})

	t.Run("should record metrics", func(t *testing.T) {
		extrFactory := extractor.NewFactory()
		extrFactory.Set("test-extractor", newTestExtractor)
		procFactory := processor.NewFactory()
		procFactory.Set("test-processor", newTestProcessor)
		mSink := new(mockSink)
		mSink.On("Sink", mock.Anything, mock.Anything).Return(nil)
		sinkFactory := sink.NewFactory()
		sinkFactory.Set("mock-sink", newTestSink(mSink))
		monitor := new(mockMonitor)
		monitor.On("RecordRun", rcp, mock.AnythingOfType("int"), true).Once()
		defer monitor.AssertExpectations(t)

		r := recipe.NewRunner(extrFactory, procFactory, sinkFactory, monitor)
		_, err := r.Run(rcp)
		if err != nil {
			t.Error(err.Error())
		}
	})
}

func TestRunnerRunMultiple(t *testing.T) {
	validRecipe2 := validRecipe
	validRecipe2.Name = "sample-2"

	t.Run("should return list of failed recipes when finished", func(t *testing.T) {
		failedRecipe := recipe.Recipe{
			Name: "failedRecipe",
		}
		recipeList := []recipe.Recipe{validRecipe, failedRecipe, validRecipe2}

		extrFactory := extractor.NewFactory()
		extrFactory.Set("test-extractor", newTestExtractor)
		procFactory := processor.NewFactory()
		procFactory.Set("test-processor", newTestProcessor)
		mSink := new(mockSink)
		mSink.On("Sink", mock.Anything, mock.Anything).Return(nil)
		sinkFactory := sink.NewFactory()
		sinkFactory.Set("mock-sink", newTestSink(mSink))

		r := recipe.NewRunner(extrFactory, procFactory, sinkFactory, nil)
		faileds, err := r.RunMultiple(recipeList)
		assert.Nil(t, err)

		expected := []string{
			failedRecipe.Name,
		}
		assert.Equal(t, expected, faileds)
	})

	t.Run("should run all extractors, processors and sinks for all recipes", func(t *testing.T) {
		validRecipe2.Sinks = []recipe.SinkRecipe{
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

		recipeList := []recipe.Recipe{validRecipe, validRecipe2}

		extrFactory := extractor.NewFactory()
		extrFactory.Set("test-extractor", newTestExtractor)
		procFactory := processor.NewFactory()
		procFactory.Set("test-processor", newTestProcessor)
		sink1 := new(mockSink)
		sink2 := new(mockSink)
		sink1.On("Sink", finalData, validRecipe.Sinks[0].Config).Return(nil).Once()
		defer sink1.AssertExpectations(t)
		sink2.On("Sink", finalData, validRecipe2.Sinks[0].Config).Return(nil).Once()
		defer sink2.AssertExpectations(t)
		sinkFactory := sink.NewFactory()
		sinkFactory.Set("mock-sink", newTestSink(sink1))
		sinkFactory.Set("mock-sink-2", newTestSink(sink2))

		r := recipe.NewRunner(extrFactory, procFactory, sinkFactory, nil)
		faileds, err := r.RunMultiple(recipeList)
		assert.Nil(t, err)
		assert.Equal(t, []string{}, faileds)
	})
}

type testProcessor struct{}

func newTestProcessor() processor.Processor {
	return &testProcessor{}
}

func (t *testProcessor) Process(data []map[string]interface{}, config map[string]interface{}) ([]map[string]interface{}, error) {
	for _, d := range data {
		d["test"] = true
	}

	return data, nil
}

type testExtractor struct{}

func newTestExtractor() extractor.Extractor {
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

func newTestSink(s sink.Sink) sink.FactoryFn {
	return func() sink.Sink {
		return s
	}
}

func (m *mockSink) Sink(data []map[string]interface{}, config map[string]interface{}) error {
	args := m.Called(data, config)
	return args.Error(0)
}

type mockMonitor struct {
	mock.Mock
}

func (m *mockMonitor) RecordRun(recipe recipe.Recipe, durationInMs int, success bool) {
	m.Called(recipe, durationInMs, success)
}
