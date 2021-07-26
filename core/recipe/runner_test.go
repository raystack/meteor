package recipe_test

import (
	"context"
	"errors"
	"testing"

	"github.com/odpf/meteor/core"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/core/processor"
	"github.com/odpf/meteor/core/recipe"
	"github.com/odpf/meteor/core/sink"
	"github.com/odpf/meteor/proto/odpf/meta"
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

var finalData = []meta.Table{
	{
		Urn: "foo-1-bar",
	},
	{
		Urn: "foo-2-bar",
	},
}

func TestRunnerRun(t *testing.T) {
	rcp := validRecipe

	t.Run("should return error on failed run", func(t *testing.T) {
		extrFactory := extractor.NewFactory()
		procFactory := processor.NewFactory()
		sinkFactory := sink.NewFactory()

		r := recipe.NewRunner(extrFactory, procFactory, sinkFactory, nil)
		err := r.Run(rcp)
		assert.NotNil(t, err)
	})

	t.Run("should run extractor, processors and sinks", func(t *testing.T) {
		mSink := new(mockPassthroughSink)

		extrFactory := extractor.NewFactory()
		extrFactory.Register("test-extractor", newMockExtractor)
		procFactory := processor.NewFactory()
		procFactory.Register("test-processor", newMockProcessor)
		sinkFactory := sink.NewFactory()
		sinkFactory.Register("mock-sink", newMockSinkFn(mSink))

		r := recipe.NewRunner(extrFactory, procFactory, sinkFactory, nil)
		err := r.Run(rcp)
		if err != nil {
			t.Error(err.Error())
		}
		result := mSink.GetResult()
		assert.Equal(t, finalData, result)
	})

	t.Run("should record metrics", func(t *testing.T) {
		extrFactory := extractor.NewFactory()
		extrFactory.Register("test-extractor", newMockExtractor)
		procFactory := processor.NewFactory()
		procFactory.Register("test-processor", newMockProcessor)
		mSink := new(mockPassthroughSink)
		sinkFactory := sink.NewFactory()
		sinkFactory.Register("mock-sink", newMockSinkFn(mSink))
		monitor := new(mockMonitor)
		monitor.On("RecordRun", rcp, mock.AnythingOfType("int"), true).Once()
		defer monitor.AssertExpectations(t)

		r := recipe.NewRunner(extrFactory, procFactory, sinkFactory, monitor)
		err := r.Run(rcp)
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
		extrFactory.Register("test-extractor", newMockExtractor)
		procFactory := processor.NewFactory()
		procFactory.Register("test-processor", newMockProcessor)
		sinkFactory := sink.NewFactory()
		sinkFactory.Register("mock-sink", newMockSinkFn(new(mockPassthroughSink)))

		r := recipe.NewRunner(extrFactory, procFactory, sinkFactory, nil)
		fails, err := r.RunMultiple(recipeList)
		assert.Nil(t, err)

		expected := []string{
			failedRecipe.Name,
		}
		assert.Equal(t, expected, fails)
	})

	t.Run("should run all extractors, processors and sinks for all recipes", func(t *testing.T) {
		validRecipe2.Sinks = []recipe.SinkRecipe{
			{Name: "mock-sink-2"},
		}

		recipeList := []recipe.Recipe{validRecipe, validRecipe2}

		extrFactory := extractor.NewFactory()
		extrFactory.Register("test-extractor", newMockExtractor)
		procFactory := processor.NewFactory()
		procFactory.Register("test-processor", newMockProcessor)
		sink1 := new(mockPassthroughSink)
		sink2 := new(mockPassthroughSink)
		sinkFactory := sink.NewFactory()
		sinkFactory.Register("mock-sink", newMockSinkFn(sink1))
		sinkFactory.Register("mock-sink-2", newMockSinkFn(sink2))

		r := recipe.NewRunner(extrFactory, procFactory, sinkFactory, nil)
		fails, err := r.RunMultiple(recipeList)
		assert.Nil(t, err)
		assert.Equal(t, []string{}, fails)
		assert.Equal(t, finalData, sink1.GetResult())
		assert.Equal(t, finalData, sink2.GetResult())
	})
}

// This test processor will append meta.Table.Urn with "-bar"
type mockProcessor struct{}

func newMockProcessor() core.Processor {
	return &mockProcessor{}
}

func (t *mockProcessor) Process(ctx context.Context, config map[string]interface{}, in <-chan interface{}, out chan<- interface{}) error {
	data := <-in
	tables, ok := data.([]meta.Table)
	if !ok {
		return errors.New("invalid data type")
	}

	for i := 0; i < len(tables); i++ {
		table := &tables[i]
		table.Urn = table.Urn + "-bar"
	}

	out <- tables
	return nil
}

type mockExtractor struct{}

func newMockExtractor() core.Extractor {
	return &mockExtractor{}
}

func (t *mockExtractor) Extract(ctx context.Context, config map[string]interface{}, out chan<- interface{}) error {
	out <- []meta.Table{
		{
			Urn: "foo-1",
		},
		{
			Urn: "foo-2",
		},
	}
	return nil
}

type mockPassthroughSink struct {
	result interface{}
}

func newMockSinkFn(sink core.Syncer) func() core.Syncer {
	return func() core.Syncer {
		return sink
	}
}

func (m *mockPassthroughSink) Sink(ctx context.Context, config map[string]interface{}, in <-chan interface{}) error {
	m.result = <-in
	return nil
}

func (m *mockPassthroughSink) GetResult() interface{} {
	return m.result
}

type mockMonitor struct {
	mock.Mock
}

func (m *mockMonitor) RecordRun(recipe recipe.Recipe, durationInMs int, success bool) {
	m.Called(recipe, durationInMs, success)
}
