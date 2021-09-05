package agent_test

import (
	"context"
	"errors"
	"testing"

	"github.com/odpf/meteor/agent"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/assets"
	"github.com/odpf/meteor/proto/odpf/assets/common"
	"github.com/odpf/meteor/recipe"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/test"
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

var finalData = []interface{}{
	assets.Table{
		Resource: &common.Resource{
			Urn: "foo-1-bar",
		},
	},
	assets.Table{
		Resource: &common.Resource{
			Urn: "foo-2-bar",
		},
	},
}

var extrFactory = registry.NewExtractorFactory()
var procFactory = registry.NewProcessorFactory()

func init() {
	extrFactory.Register("test-extractor", newMockExtractor)
	extrFactory.Register("failed-extractor", newFailedExtractor)

	procFactory.Register("test-processor", newMockProcessor)
	procFactory.Register("failed-processor", newFailedProcessor)
}

func TestRunnerRun(t *testing.T) {
	t.Run("should return run", func(t *testing.T) {
		r := agent.NewAgent(registry.NewExtractorFactory(), registry.NewProcessorFactory(), registry.NewSinkFactory(), nil, test.Logger)
		run := r.Run(validRecipe)
		assert.IsType(t, agent.Run{}, run)
		assert.Equal(t, validRecipe, run.Recipe)
	})

	t.Run("should return error if extractor, processors or sinks could not be found", func(t *testing.T) {
		r := agent.NewAgent(registry.NewExtractorFactory(), registry.NewProcessorFactory(), registry.NewSinkFactory(), nil, test.Logger)
		run := r.Run(validRecipe)
		assert.Error(t, run.Error)
	})

	t.Run("should return error on failed extractor", func(t *testing.T) {
		rcp := validRecipe
		rcp.Source = recipe.SourceRecipe{
			Type: "failed-extractor",
		}

		mSink := new(mockPassthroughSink)
		sinkFactory := registry.NewSinkFactory()
		sinkFactory.Register("mock-sink", newMockSinkFn(mSink))

		r := agent.NewAgent(extrFactory, procFactory, sinkFactory, nil, test.Logger)
		run := r.Run(rcp)
		assert.Error(t, run.Error)
	})

	t.Run("should return error on failed processor", func(t *testing.T) {
		rcp := validRecipe
		rcp.Processors = []recipe.ProcessorRecipe{
			{Name: "failed-processor"},
		}

		mSink := new(mockPassthroughSink)
		sinkFactory := registry.NewSinkFactory()
		sinkFactory.Register("mock-sink", newMockSinkFn(mSink))

		r := agent.NewAgent(extrFactory, procFactory, sinkFactory, nil, test.Logger)
		run := r.Run(rcp)
		assert.Error(t, run.Error)
	})

	t.Run("should run extractor, processors and sinks", func(t *testing.T) {
		mSink := new(mockPassthroughSink)
		sinkFactory := registry.NewSinkFactory()
		sinkFactory.Register("mock-sink", newMockSinkFn(mSink))

		r := agent.NewAgent(extrFactory, procFactory, sinkFactory, nil, test.Logger)
		run := r.Run(validRecipe)
		assert.NoError(t, run.Error)

		result := mSink.GetResult()
		assert.Equal(t, finalData, result)
	})

	t.Run("should record metrics", func(t *testing.T) {
		mSink := new(mockPassthroughSink)
		sinkFactory := registry.NewSinkFactory()
		sinkFactory.Register("mock-sink", newMockSinkFn(mSink))
		monitor := new(mockMonitor)
		monitor.On("RecordRun", validRecipe, mock.AnythingOfType("int"), true).Once()
		defer monitor.AssertExpectations(t)

		r := agent.NewAgent(extrFactory, procFactory, sinkFactory, monitor, test.Logger)
		run := r.Run(validRecipe)
		assert.NoError(t, run.Error)
	})
}

func TestRunnerRunMultiple(t *testing.T) {
	validRecipe2 := validRecipe
	validRecipe2.Name = "sample-2"

	t.Run("should return list of runs when finished", func(t *testing.T) {
		failedProcessorName := "failed-processor"
		failedRecipe := validRecipe
		failedRecipe.Name = "failedRecipe"
		failedRecipe.Processors = []recipe.ProcessorRecipe{
			{Name: failedProcessorName},
		}
		recipeList := []recipe.Recipe{validRecipe, failedRecipe, validRecipe2}

		sinkFactory := registry.NewSinkFactory()
		sinkFactory.Register("mock-sink", newMockSinkFn(new(mockPassthroughSink)))

		r := agent.NewAgent(extrFactory, procFactory, sinkFactory, nil, test.Logger)
		runs := r.RunMultiple(recipeList)

		assert.Len(t, runs, len(recipeList))
		for _, run := range runs {
			if run.Recipe.Name == failedRecipe.Name {
				assert.Error(t, run.Error)
			} else {
				assert.NoError(t, run.Error)
			}
		}
	})

	t.Run("should run all extractors, processors and sinks for all recipes", func(t *testing.T) {
		validRecipe2.Sinks = []recipe.SinkRecipe{
			{Name: "mock-sink-2"},
		}

		recipeList := []recipe.Recipe{validRecipe, validRecipe2}

		sink1 := new(mockPassthroughSink)
		sink2 := new(mockPassthroughSink)
		sinkFactory := registry.NewSinkFactory()
		sinkFactory.Register("mock-sink", newMockSinkFn(sink1))
		sinkFactory.Register("mock-sink-2", newMockSinkFn(sink2))

		r := agent.NewAgent(extrFactory, procFactory, sinkFactory, nil, test.Logger)
		r.RunMultiple(recipeList)

		assert.Equal(t, finalData, sink1.GetResult())
		assert.Equal(t, finalData, sink2.GetResult())
	})
}

type mockExtractor struct{}

func newMockExtractor() plugins.Extractor {
	return &mockExtractor{}
}

func (t *mockExtractor) Info() plugins.Info {
	return plugins.Info{}
}

func (t *mockExtractor) Validate(config map[string]interface{}) error {
	return nil
}

func (t *mockExtractor) Extract(ctx context.Context, config map[string]interface{}, out chan<- interface{}) error {
	data := []assets.Table{
		{
			Resource: &common.Resource{
				Urn: "foo-1",
			},
		},
		{
			Resource: &common.Resource{
				Urn: "foo-2",
			},
		},
	}

	for _, d := range data {
		out <- d
	}

	return nil
}

// This test processor will append assets.Table.Urn with "-bar"
type mockProcessor struct{}

func newMockProcessor() plugins.Processor {
	return &mockProcessor{}
}

func (t *mockProcessor) Info() plugins.Info {
	return plugins.Info{}
}

func (t *mockProcessor) Validate(config map[string]interface{}) error {
	return nil
}

func (t *mockProcessor) Process(ctx context.Context, config map[string]interface{}, in <-chan interface{}, out chan<- interface{}) error {
	for data := range in {
		table, ok := data.(assets.Table)
		if !ok {
			return errors.New("invalid data type")
		}

		table.Resource.Urn = table.Resource.Urn + "-bar"

		out <- table
	}

	return nil
}

type mockPassthroughSink struct {
	result []interface{}
}

func newMockSinkFn(sink plugins.Syncer) func() plugins.Syncer {
	return func() plugins.Syncer {
		return sink
	}
}

func (m *mockPassthroughSink) Info() plugins.Info {
	return plugins.Info{}
}

func (m *mockPassthroughSink) Validate(config map[string]interface{}) error {
	return nil
}

func (m *mockPassthroughSink) Sink(ctx context.Context, config map[string]interface{}, in <-chan interface{}) error {
	if m.result == nil {
		m.result = []interface{}{}
	}

	for data := range in {
		m.result = append(m.result, data)
	}

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

type failedProcessor struct{}

func newFailedProcessor() plugins.Processor {
	return &failedProcessor{}
}

func (t *failedProcessor) Info() plugins.Info {
	return plugins.Info{}
}

func (t *failedProcessor) Validate(config map[string]interface{}) error {
	return nil
}

func (t *failedProcessor) Process(ctx context.Context, config map[string]interface{}, in <-chan interface{}, out chan<- interface{}) error {
	for range in {
		return errors.New("failed processor")
	}

	return nil
}

type failedExtractor struct{}

func newFailedExtractor() plugins.Extractor {
	return &failedExtractor{}
}

func (t *failedExtractor) Info() plugins.Info {
	return plugins.Info{}
}

func (t *failedExtractor) Validate(config map[string]interface{}) error {
	return nil
}

func (e *failedExtractor) Extract(ctx context.Context, config map[string]interface{}, out chan<- interface{}) error {
	out <- assets.Table{
		Resource: &common.Resource{
			Urn: "id-1",
		},
	}
	return errors.New("failed extractor")
}
