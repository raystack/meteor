package agent_test

import (
	"errors"
	"testing"

	"github.com/odpf/meteor/agent"
	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/recipe"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/test"
	"github.com/odpf/meteor/test/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var mockCtx = mock.AnythingOfType("*context.emptyCtx")

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
		{Name: "test-sink", Config: map[string]interface{}{
			"url": "http://localhost:3000/data",
		}},
	},
}

var finalData = []models.Record{
	models.NewRecord(&assets.Table{
		Resource: &common.Resource{
			Urn: "foo-1-bar",
		},
	}),
	models.NewRecord(&assets.Table{
		Resource: &common.Resource{
			Urn: "foo-2-bar",
		},
	}),
}

func TestRunnerRun(t *testing.T) {
	t.Run("should return run", func(t *testing.T) {
		r := agent.NewAgent(registry.NewExtractorFactory(), registry.NewProcessorFactory(), registry.NewSinkFactory(), nil, test.Logger)
		run := r.Run(validRecipe)
		assert.IsType(t, agent.Run{}, run)
		assert.Equal(t, validRecipe, run.Recipe)
	})

	t.Run("should return error if extractor could not be found", func(t *testing.T) {
		proc := mocks.NewProcessor()
		pf := registry.NewProcessorFactory()
		pf.Register("test-processor", newProcessor(proc))

		sink := mocks.NewSink()
		sf := registry.NewSinkFactory()
		sf.Register("test-sink", newSink(sink))

		r := agent.NewAgent(registry.NewExtractorFactory(), pf, sf, nil, test.Logger)
		run := r.Run(validRecipe)
		assert.Error(t, run.Error)
	})

	t.Run("should return error if processor could not be found", func(t *testing.T) {
		extr := mocks.NewExtractor()
		extr.On("Init", mockCtx, validRecipe.Source.Config).Return(nil).Once()
		defer extr.AssertExpectations(t)
		ef := registry.NewExtractorFactory()
		ef.Register("test-extractor", newExtractor(extr))

		sink := mocks.NewSink()
		sf := registry.NewSinkFactory()
		sf.Register("test-sink", newSink(sink))

		r := agent.NewAgent(ef, registry.NewProcessorFactory(), sf, nil, test.Logger)
		run := r.Run(validRecipe)
		assert.Error(t, run.Error)
	})

	t.Run("should return error if sink could not be found", func(t *testing.T) {
		extr := mocks.NewExtractor()
		extr.On("Init", mockCtx, validRecipe.Source.Config).Return(nil).Once()
		defer extr.AssertExpectations(t)
		ef := registry.NewExtractorFactory()
		ef.Register("test-extractor", newExtractor(extr))

		proc := mocks.NewProcessor()
		proc.On("Init", mockCtx, validRecipe.Processors[0].Config).Return(nil).Once()
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		pf.Register("test-processor", newProcessor(proc))

		r := agent.NewAgent(ef, pf, registry.NewSinkFactory(), nil, test.Logger)
		run := r.Run(validRecipe)
		assert.Error(t, run.Error)
	})

	t.Run("should return error when initiating extractor fails", func(t *testing.T) {
		extr := mocks.NewExtractor()
		extr.On("Init", mockCtx, validRecipe.Source.Config).Return(errors.New("some error")).Once()
		defer extr.AssertExpectations(t)
		ef := registry.NewExtractorFactory()
		ef.Register("test-extractor", newExtractor(extr))

		proc := mocks.NewProcessor()
		pf := registry.NewProcessorFactory()
		pf.Register("test-processor", newProcessor(proc))

		sink := mocks.NewSink()
		sf := registry.NewSinkFactory()
		sf.Register("test-sink", newSink(sink))

		r := agent.NewAgent(ef, pf, sf, nil, test.Logger)
		run := r.Run(validRecipe)
		assert.Error(t, run.Error)
	})

	t.Run("should return error when initiating processor fails", func(t *testing.T) {
		extr := mocks.NewExtractor()
		extr.On("Init", mockCtx, validRecipe.Source.Config).Return(nil).Once()
		defer extr.AssertExpectations(t)
		ef := registry.NewExtractorFactory()
		ef.Register("test-extractor", newExtractor(extr))

		proc := mocks.NewProcessor()
		proc.On("Init", mockCtx, validRecipe.Processors[0].Config).Return(errors.New("some error")).Once()
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		pf.Register("test-processor", newProcessor(proc))

		sink := mocks.NewSink()
		sf := registry.NewSinkFactory()
		sf.Register("test-sink", newSink(sink))

		r := agent.NewAgent(ef, pf, sf, nil, test.Logger)
		run := r.Run(validRecipe)
		assert.Error(t, run.Error)
	})

	t.Run("should return error when initiating sink fails", func(t *testing.T) {
		extr := mocks.NewExtractor()
		extr.On("Init", mockCtx, validRecipe.Source.Config).Return(nil).Once()
		defer extr.AssertExpectations(t)
		ef := registry.NewExtractorFactory()
		ef.Register("test-extractor", newExtractor(extr))

		proc := mocks.NewProcessor()
		proc.On("Init", mockCtx, validRecipe.Processors[0].Config).Return(nil).Once()
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		pf.Register("test-processor", newProcessor(proc))

		sink := mocks.NewSink()
		sink.On("Init", mockCtx, validRecipe.Sinks[0].Config).Return(errors.New("some error")).Once()
		defer sink.AssertExpectations(t)
		sf := registry.NewSinkFactory()
		sf.Register("test-sink", newSink(sink))

		r := agent.NewAgent(ef, pf, sf, nil, test.Logger)
		run := r.Run(validRecipe)
		assert.Error(t, run.Error)
	})

	t.Run("should return error when extracting fails", func(t *testing.T) {
		extr := mocks.NewExtractor()
		extr.On("Init", mockCtx, validRecipe.Source.Config).Return(nil).Once()
		extr.On("Extract", mockCtx, mock.AnythingOfType("plugins.Emit")).Return(errors.New("some error")).Once()
		ef := registry.NewExtractorFactory()
		ef.Register("test-extractor", newExtractor(extr))

		proc := mocks.NewProcessor()
		proc.On("Init", mockCtx, validRecipe.Processors[0].Config).Return(nil).Once()
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		pf.Register("test-processor", newProcessor(proc))

		sink := mocks.NewSink()
		sink.On("Init", mockCtx, validRecipe.Sinks[0].Config).Return(nil).Once()
		defer sink.AssertExpectations(t)
		sf := registry.NewSinkFactory()
		sf.Register("test-sink", newSink(sink))

		r := agent.NewAgent(ef, pf, sf, nil, test.Logger)
		run := r.Run(validRecipe)
		assert.Error(t, run.Error)
	})

	t.Run("should return error when processing fails", func(t *testing.T) {
		data := []models.Record{
			models.NewRecord(&assets.Table{}),
		}

		extr := mocks.NewExtractor()
		extr.SetEmit(data)
		extr.On("Init", mockCtx, validRecipe.Source.Config).Return(nil).Once()
		extr.On("Extract", mockCtx, mock.AnythingOfType("plugins.Emit")).Return(nil).Once()
		ef := registry.NewExtractorFactory()
		ef.Register("test-extractor", newExtractor(extr))

		proc := mocks.NewProcessor()
		proc.On("Init", mockCtx, validRecipe.Processors[0].Config).Return(nil).Once()
		proc.On("Process", mockCtx, data[0]).Return(data[0], errors.New("some error")).Once()
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		pf.Register("test-processor", newProcessor(proc))

		sink := mocks.NewSink()
		sink.On("Init", mockCtx, validRecipe.Sinks[0].Config).Return(nil).Once()
		defer sink.AssertExpectations(t)
		sf := registry.NewSinkFactory()
		sf.Register("test-sink", newSink(sink))

		r := agent.NewAgent(ef, pf, sf, nil, test.Logger)
		run := r.Run(validRecipe)
		assert.Error(t, run.Error)
	})

	t.Run("should return error when sink fails", func(t *testing.T) {
		data := []models.Record{
			models.NewRecord(&assets.Table{}),
		}

		extr := mocks.NewExtractor()
		extr.SetEmit(data)
		extr.On("Init", mockCtx, validRecipe.Source.Config).Return(nil).Once()
		extr.On("Extract", mockCtx, mock.AnythingOfType("plugins.Emit")).Return(nil)
		ef := registry.NewExtractorFactory()
		ef.Register("test-extractor", newExtractor(extr))

		proc := mocks.NewProcessor()
		proc.On("Init", mockCtx, validRecipe.Processors[0].Config).Return(nil).Once()
		proc.On("Process", mockCtx, data[0]).Return(data[0], nil)
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		pf.Register("test-processor", newProcessor(proc))

		sink := mocks.NewSink()
		sink.On("Init", mockCtx, validRecipe.Sinks[0].Config).Return(nil).Once()
		sink.On("Sink", mockCtx, data).Return(errors.New("some error"))
		defer sink.AssertExpectations(t)
		sf := registry.NewSinkFactory()
		sf.Register("test-sink", newSink(sink))

		r := agent.NewAgent(ef, pf, sf, nil, test.Logger)
		run := r.Run(validRecipe)
		assert.Error(t, run.Error)
	})

	t.Run("should return run on success", func(t *testing.T) {
		data := []models.Record{
			models.NewRecord(&assets.Table{}),
		}

		extr := mocks.NewExtractor()
		extr.SetEmit(data)
		extr.On("Init", mockCtx, validRecipe.Source.Config).Return(nil).Once()
		extr.On("Extract", mockCtx, mock.AnythingOfType("plugins.Emit")).Return(nil)
		ef := registry.NewExtractorFactory()
		ef.Register("test-extractor", newExtractor(extr))

		proc := mocks.NewProcessor()
		proc.On("Init", mockCtx, validRecipe.Processors[0].Config).Return(nil).Once()
		proc.On("Process", mockCtx, data[0]).Return(data[0], nil)
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		pf.Register("test-processor", newProcessor(proc))

		sink := mocks.NewSink()
		sink.On("Init", mockCtx, validRecipe.Sinks[0].Config).Return(nil).Once()
		sink.On("Sink", mockCtx, data).Return(nil)
		defer sink.AssertExpectations(t)
		sf := registry.NewSinkFactory()
		sf.Register("test-sink", newSink(sink))

		r := agent.NewAgent(ef, pf, sf, nil, test.Logger)
		run := r.Run(validRecipe)
		assert.NoError(t, run.Error)
		assert.Equal(t, validRecipe, run.Recipe)
	})

	t.Run("should collect run metrics", func(t *testing.T) {
		data := []models.Record{
			models.NewRecord(&assets.Table{}),
		}

		extr := mocks.NewExtractor()
		extr.SetEmit(data)
		extr.On("Init", mockCtx, validRecipe.Source.Config).Return(nil).Once()
		extr.On("Extract", mockCtx, mock.AnythingOfType("plugins.Emit")).Return(nil)
		ef := registry.NewExtractorFactory()
		ef.Register("test-extractor", newExtractor(extr))

		proc := mocks.NewProcessor()
		proc.On("Init", mockCtx, validRecipe.Processors[0].Config).Return(nil).Once()
		proc.On("Process", mockCtx, data[0]).Return(data[0], nil)
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		pf.Register("test-processor", newProcessor(proc))

		sink := mocks.NewSink()
		sink.On("Init", mockCtx, validRecipe.Sinks[0].Config).Return(nil).Once()
		sink.On("Sink", mockCtx, data).Return(nil)
		defer sink.AssertExpectations(t)
		sf := registry.NewSinkFactory()
		sf.Register("test-sink", newSink(sink))

		monitor := newMockMonitor()
		monitor.On("RecordRun", validRecipe, mock.AnythingOfType("int"), true).Once()
		defer monitor.AssertExpectations(t)

		r := agent.NewAgent(ef, pf, sf, monitor, test.Logger)
		run := r.Run(validRecipe)
		assert.NoError(t, run.Error)
		assert.Equal(t, validRecipe, run.Recipe)
	})
}

func TestRunnerRunMultiple(t *testing.T) {
	t.Run("should return list of runs when finished", func(t *testing.T) {
		validRecipe2 := validRecipe
		validRecipe2.Name = "sample-2"
		recipeList := []recipe.Recipe{validRecipe, validRecipe2}
		data := []models.Record{
			models.NewRecord(&assets.Table{}),
		}

		extr := mocks.NewExtractor()
		extr.SetEmit(data)
		extr.On("Init", mockCtx, validRecipe.Source.Config).Return(nil)
		extr.On("Extract", mockCtx, mock.AnythingOfType("plugins.Emit")).Return(nil)
		ef := registry.NewExtractorFactory()
		ef.Register("test-extractor", newExtractor(extr))

		proc := mocks.NewProcessor()
		proc.On("Init", mockCtx, validRecipe.Processors[0].Config).Return(nil)
		proc.On("Process", mockCtx, data[0]).Return(data[0], nil)
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		pf.Register("test-processor", newProcessor(proc))

		sink := mocks.NewSink()
		sink.On("Init", mockCtx, validRecipe.Sinks[0].Config).Return(nil)
		sink.On("Sink", mockCtx, data).Return(nil)
		defer sink.AssertExpectations(t)
		sf := registry.NewSinkFactory()
		sf.Register("test-sink", newSink(sink))

		r := agent.NewAgent(ef, pf, sf, nil, test.Logger)
		runs := r.RunMultiple(recipeList)

		assert.Len(t, runs, len(recipeList))
		assert.Equal(t, []agent.Run{
			{Recipe: validRecipe},
			{Recipe: validRecipe2},
		}, runs)
	})
}

func newExtractor(extr plugins.Extractor) func() plugins.Extractor {
	return func() plugins.Extractor {
		return extr
	}
}

func newProcessor(proc plugins.Processor) func() plugins.Processor {
	return func() plugins.Processor {
		return proc
	}
}

func newSink(sink plugins.Syncer) func() plugins.Syncer {
	return func() plugins.Syncer {
		return sink
	}
}

type mockMonitor struct {
	mock.Mock
}

func newMockMonitor() *mockMonitor {
	return &mockMonitor{}
}

func (m *mockMonitor) RecordRun(recipe recipe.Recipe, durationInMs int, success bool) {
	m.Called(recipe, durationInMs, success)
}
