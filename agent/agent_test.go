package agent_test

import (
	"context"
	"errors"
	"testing"
	"time"

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
		r := agent.NewAgent(agent.Config{
			ExtractorFactory: registry.NewExtractorFactory(),
			ProcessorFactory: registry.NewProcessorFactory(),
			SinkFactory:      registry.NewSinkFactory(),
			Logger:           test.Logger,
		})
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

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: registry.NewExtractorFactory(),
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           test.Logger,
		})
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

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: registry.NewProcessorFactory(),
			SinkFactory:      sf,
			Logger:           test.Logger,
		})
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

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      registry.NewSinkFactory(),
			Logger:           test.Logger,
		})
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

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           test.Logger,
		})
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

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           test.Logger,
		})
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

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           test.Logger,
		})
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

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           test.Logger,
		})
		run := r.Run(validRecipe)
		assert.Error(t, run.Error)
	})

	t.Run("should return error when extractor panicing", func(t *testing.T) {
		extr := new(panicExtractor)
		extr.On("Init", mockCtx, validRecipe.Source.Config).Return(nil).Once()
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

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           test.Logger,
		})
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

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           test.Logger,
		})
		run := r.Run(validRecipe)
		assert.Error(t, run.Error)
	})

	t.Run("should return error when processing panics", func(t *testing.T) {
		data := []models.Record{
			models.NewRecord(&assets.Table{}),
		}

		extr := mocks.NewExtractor()
		extr.SetEmit(data)
		extr.On("Init", mockCtx, validRecipe.Source.Config).Return(nil).Once()
		extr.On("Extract", mockCtx, mock.AnythingOfType("plugins.Emit")).Return(nil).Once()
		ef := registry.NewExtractorFactory()
		ef.Register("test-extractor", newExtractor(extr))

		proc := new(panicProcessor)
		proc.On("Init", mockCtx, validRecipe.Processors[0].Config).Return(nil).Once()
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		pf.Register("test-processor", newProcessor(proc))

		sink := mocks.NewSink()
		sink.On("Init", mockCtx, validRecipe.Sinks[0].Config).Return(nil).Once()
		defer sink.AssertExpectations(t)
		sf := registry.NewSinkFactory()
		sf.Register("test-sink", newSink(sink))

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           test.Logger,
		})
		run := r.Run(validRecipe)
		assert.Error(t, run.Error)
	})

	t.Run("should not return error when sink fails", func(t *testing.T) {
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

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           test.Logger,
		})
		run := r.Run(validRecipe)
		assert.NoError(t, run.Error)
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

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           test.Logger,
		})
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

		monitor_run := agent.Run{Recipe: validRecipe, RecordCount: 1, Success: true}
		monitor := newMockMonitor()
		monitor.On("RecordRun", monitor_run).Once()
		defer monitor.AssertExpectations(t)

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Monitor:          monitor,
			Logger:           test.Logger,
		})
		run := r.Run(validRecipe)
		assert.NoError(t, run.Error)
		assert.Equal(t, validRecipe, run.Recipe)
	})

	t.Run("should retry if sink returns retry error", func(t *testing.T) {
		err := errors.New("some-error")
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
		sink.On("Sink", mockCtx, data).Return(plugins.NewRetryError(err)).Once()
		sink.On("Sink", mockCtx, data).Return(plugins.NewRetryError(err)).Once()
		sink.On("Sink", mockCtx, data).Return(nil)
		defer sink.AssertExpectations(t)
		sf := registry.NewSinkFactory()
		sf.Register("test-sink", newSink(sink))

		r := agent.NewAgent(agent.Config{
			ExtractorFactory:     ef,
			ProcessorFactory:     pf,
			SinkFactory:          sf,
			Logger:               test.Logger,
			RetryTimes:           2,                    // need to retry "at least" 2 times since Sink returns RetryError twice
			RetryInitialInterval: 1 * time.Millisecond, // this is to override default retry interval to reduce test time
		})
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

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           test.Logger,
		})
		runs := r.RunMultiple(recipeList)

		assert.Len(t, runs, len(recipeList))
		assert.Equal(t, []agent.Run{
			{Recipe: validRecipe, RecordCount: len(data), Success: true},
			{Recipe: validRecipe2, RecordCount: len(data), Success: true},
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

func (m *mockMonitor) RecordRun(run agent.Run) {
	m.Called(run)
}

type panicExtractor struct {
	mocks.Extractor
}

func (e *panicExtractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	panic("panicing")
}

type panicProcessor struct {
	mocks.Processor
}

func (p *panicProcessor) Process(ctx context.Context, src models.Record) (dst models.Record, err error) {
	panic("panicing")
}
