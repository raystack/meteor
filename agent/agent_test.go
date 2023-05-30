package agent_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/goto/meteor/agent"
	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	_ "github.com/goto/meteor/plugins/extractors" // populate extractors registry
	_ "github.com/goto/meteor/plugins/processors" // populate processors registry
	_ "github.com/goto/meteor/plugins/sinks"      // populate sinks registry
	"github.com/goto/meteor/recipe"
	"github.com/goto/meteor/registry"
	"github.com/goto/meteor/test/mocks"
	"github.com/goto/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	mockCtx = mock.AnythingOfType("*context.emptyCtx")
	ctx     = context.TODO()
)

var validRecipe = recipe.Recipe{
	Name: "sample",
	Source: recipe.PluginRecipe{
		Name: "test-extractor",
	},
	Processors: []recipe.PluginRecipe{
		{Name: "test-processor", Config: map[string]interface{}{
			"proc-foo": "proc-bar",
		}},
	},
	Sinks: []recipe.PluginRecipe{
		{Name: "test-sink", Config: map[string]interface{}{
			"url": "http://localhost:3000/data",
		}},
	},
}

func TestAgentRun(t *testing.T) {
	t.Run("should return run", func(t *testing.T) {
		r := agent.NewAgent(agent.Config{
			ExtractorFactory: registry.NewExtractorFactory(),
			ProcessorFactory: registry.NewProcessorFactory(),
			SinkFactory:      registry.NewSinkFactory(),
			Logger:           utils.Logger,
		})
		run := r.Run(ctx, validRecipe)
		assert.IsType(t, agent.Run{}, run)
		assert.Equal(t, validRecipe, run.Recipe)
	})

	t.Run("should return error if extractor could not be found", func(t *testing.T) {
		proc := mocks.NewProcessor()
		pf := registry.NewProcessorFactory()
		if err := pf.Register("test-processor", newProcessor(proc)); err != nil {
			t.Fatal(err)
		}

		sink := mocks.NewSink()
		sf := registry.NewSinkFactory()
		if err := sf.Register("test-sink", newSink(sink)); err != nil {
			t.Fatal(err)
		}

		monitor := newMockMonitor()
		monitor.On("RecordRun", mock.AnythingOfType("agent.Run")).Once()
		defer monitor.AssertExpectations(t)

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: registry.NewExtractorFactory(),
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           utils.Logger,
			Monitor:          monitor,
		})
		run := r.Run(ctx, validRecipe)
		assert.False(t, run.Success)
		assert.ErrorIs(t, run.Error, plugins.NotFoundError{Type: plugins.PluginTypeExtractor, Name: "test-extractor"})
	})

	t.Run("should return error if processor could not be found", func(t *testing.T) {
		extr := mocks.NewExtractor()
		extr.On("Init", mockCtx, buildPluginConfig(validRecipe.Source)).Return(nil).Once()
		defer extr.AssertExpectations(t)
		ef := registry.NewExtractorFactory()
		if err := ef.Register("test-extractor", newExtractor(extr)); err != nil {
			t.Fatal(err)
		}

		sink := mocks.NewSink()
		sf := registry.NewSinkFactory()
		if err := sf.Register("test-sink", newSink(sink)); err != nil {
			t.Fatal(err)
		}

		monitor := newMockMonitor()
		monitor.On("RecordRun", mock.AnythingOfType("agent.Run")).Once()
		defer monitor.AssertExpectations(t)

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: registry.NewProcessorFactory(),
			SinkFactory:      sf,
			Logger:           utils.Logger,
			Monitor:          monitor,
		})
		run := r.Run(ctx, validRecipe)
		assert.False(t, run.Success)
		assert.Error(t, run.Error)
	})

	t.Run("should return error if sink could not be found", func(t *testing.T) {
		extr := mocks.NewExtractor()
		extr.On("Init", mockCtx, buildPluginConfig(validRecipe.Source)).Return(nil).Once()
		defer extr.AssertExpectations(t)
		ef := registry.NewExtractorFactory()
		if err := ef.Register("test-extractor", newExtractor(extr)); err != nil {
			t.Fatal(err)
		}

		proc := mocks.NewProcessor()
		proc.On("Init", mockCtx, buildPluginConfig(validRecipe.Processors[0])).Return(nil).Once()
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		if err := pf.Register("test-processor", newProcessor(proc)); err != nil {
			t.Fatal(err)
		}

		monitor := newMockMonitor()
		monitor.On("RecordRun", mock.AnythingOfType("agent.Run")).Once()
		defer monitor.AssertExpectations(t)

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      registry.NewSinkFactory(),
			Logger:           utils.Logger,
			Monitor:          monitor,
		})
		run := r.Run(ctx, validRecipe)
		assert.False(t, run.Success)
		assert.Error(t, run.Error)
	})

	t.Run("should return error when initiating extractor fails", func(t *testing.T) {
		extr := mocks.NewExtractor()
		extr.On("Init", mockCtx, buildPluginConfig(validRecipe.Source)).Return(errors.New("some error")).Once()
		defer extr.AssertExpectations(t)
		ef := registry.NewExtractorFactory()
		if err := ef.Register("test-extractor", newExtractor(extr)); err != nil {
			t.Fatal(err)
		}

		proc := mocks.NewProcessor()
		pf := registry.NewProcessorFactory()
		if err := pf.Register("test-processor", newProcessor(proc)); err != nil {
			t.Fatal(err)
		}

		sink := mocks.NewSink()
		sf := registry.NewSinkFactory()
		if err := sf.Register("test-sink", newSink(sink)); err != nil {
			t.Fatal(err)
		}

		monitor := newMockMonitor()
		monitor.On("RecordRun", mock.AnythingOfType("agent.Run")).Once()
		defer monitor.AssertExpectations(t)

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           utils.Logger,
			Monitor:          monitor,
		})
		run := r.Run(ctx, validRecipe)
		assert.False(t, run.Success)
		assert.Error(t, run.Error)
	})

	t.Run("should return error when initiating processor fails", func(t *testing.T) {
		extr := mocks.NewExtractor()
		extr.On("Init", mockCtx, buildPluginConfig(validRecipe.Source)).Return(nil).Once()
		defer extr.AssertExpectations(t)
		ef := registry.NewExtractorFactory()
		if err := ef.Register("test-extractor", newExtractor(extr)); err != nil {
			t.Fatal(err)
		}

		proc := mocks.NewProcessor()
		proc.On("Init", mockCtx, buildPluginConfig(validRecipe.Processors[0])).Return(errors.New("some error")).Once()
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		if err := pf.Register("test-processor", newProcessor(proc)); err != nil {
			t.Fatal(err)
		}

		sink := mocks.NewSink()
		sf := registry.NewSinkFactory()
		if err := sf.Register("test-sink", newSink(sink)); err != nil {
			t.Fatal(err)
		}

		monitor := newMockMonitor()
		monitor.On("RecordRun", mock.AnythingOfType("agent.Run")).Once()
		defer monitor.AssertExpectations(t)

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           utils.Logger,
			Monitor:          monitor,
		})
		run := r.Run(ctx, validRecipe)
		assert.False(t, run.Success)
		assert.Error(t, run.Error)
	})

	t.Run("should return error when initiating sink fails", func(t *testing.T) {
		extr := mocks.NewExtractor()
		extr.On("Init", mockCtx, buildPluginConfig(validRecipe.Source)).Return(nil).Once()
		defer extr.AssertExpectations(t)
		ef := registry.NewExtractorFactory()
		if err := ef.Register("test-extractor", newExtractor(extr)); err != nil {
			t.Fatal(err)
		}

		proc := mocks.NewProcessor()
		proc.On("Init", mockCtx, buildPluginConfig(validRecipe.Processors[0])).Return(nil).Once()
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		if err := pf.Register("test-processor", newProcessor(proc)); err != nil {
			t.Fatal(err)
		}

		sink := mocks.NewSink()
		sink.On("Init", mockCtx, buildPluginConfig(validRecipe.Sinks[0])).Return(errors.New("some error")).Once()
		defer sink.AssertExpectations(t)
		sf := registry.NewSinkFactory()
		if err := sf.Register("test-sink", newSink(sink)); err != nil {
			t.Fatal(err)
		}

		monitor := newMockMonitor()
		monitor.On("RecordRun", mock.AnythingOfType("agent.Run")).Once()
		defer monitor.AssertExpectations(t)

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           utils.Logger,
			Monitor:          monitor,
		})
		run := r.Run(ctx, validRecipe)
		assert.False(t, run.Success)
		assert.Error(t, run.Error)
	})

	t.Run("should return error when extracting fails", func(t *testing.T) {
		extr := mocks.NewExtractor()
		extr.On("Init", mockCtx, buildPluginConfig(validRecipe.Source)).Return(nil).Once()
		extr.On("Extract", mockCtx, mock.AnythingOfType("plugins.Emit")).Return(errors.New("some error")).Once()
		ef := registry.NewExtractorFactory()
		if err := ef.Register("test-extractor", newExtractor(extr)); err != nil {
			t.Fatal(err)
		}

		proc := mocks.NewProcessor()
		proc.On("Init", mockCtx, buildPluginConfig(validRecipe.Processors[0])).Return(nil).Once()
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		if err := pf.Register("test-processor", newProcessor(proc)); err != nil {
			t.Fatal(err)
		}

		sink := mocks.NewSink()
		sink.On("Init", mockCtx, buildPluginConfig(validRecipe.Sinks[0])).Return(nil).Once()
		sink.On("Close").Return(nil)
		defer sink.AssertExpectations(t)
		sf := registry.NewSinkFactory()
		if err := sf.Register("test-sink", newSink(sink)); err != nil {
			t.Fatal(err)
		}

		monitor := newMockMonitor()
		monitor.On("RecordRun", mock.AnythingOfType("agent.Run")).Once()
		defer monitor.AssertExpectations(t)

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           utils.Logger,
			Monitor:          monitor,
		})
		run := r.Run(ctx, validRecipe)
		assert.False(t, run.Success)
		assert.Error(t, run.Error)
	})

	t.Run("should return error when extractor panicing", func(t *testing.T) {
		extr := new(panicExtractor)
		extr.On("Init", mockCtx, buildPluginConfig(validRecipe.Source)).Return(nil).Once()
		ef := registry.NewExtractorFactory()
		if err := ef.Register("test-extractor", newExtractor(extr)); err != nil {
			t.Fatal(err)
		}

		proc := mocks.NewProcessor()
		proc.On("Init", mockCtx, buildPluginConfig(validRecipe.Processors[0])).Return(nil).Once()
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		if err := pf.Register("test-processor", newProcessor(proc)); err != nil {
			t.Fatal(err)
		}

		sink := mocks.NewSink()
		sink.On("Init", mockCtx, buildPluginConfig(validRecipe.Sinks[0])).Return(nil).Once()
		sink.On("Close").Return(nil)
		defer sink.AssertExpectations(t)
		sf := registry.NewSinkFactory()
		if err := sf.Register("test-sink", newSink(sink)); err != nil {
			t.Fatal(err)
		}

		monitor := newMockMonitor()
		monitor.On("RecordRun", mock.AnythingOfType("agent.Run")).Once()
		defer monitor.AssertExpectations(t)

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           utils.Logger,
			Monitor:          monitor,
		})
		run := r.Run(ctx, validRecipe)
		assert.False(t, run.Success)
		assert.Error(t, run.Error)
	})

	t.Run("should return error when processing fails", func(t *testing.T) {
		data := []models.Record{
			models.NewRecord(&v1beta2.Asset{}),
		}

		extr := mocks.NewExtractor()
		extr.SetEmit(data)
		extr.On("Init", mockCtx, buildPluginConfig(validRecipe.Source)).Return(nil).Once()
		extr.On("Extract", mockCtx, mock.AnythingOfType("plugins.Emit")).Return(nil).Once()
		ef := registry.NewExtractorFactory()
		if err := ef.Register("test-extractor", newExtractor(extr)); err != nil {
			t.Fatal(err)
		}

		proc := mocks.NewProcessor()
		proc.On("Init", mockCtx, buildPluginConfig(validRecipe.Processors[0])).Return(nil).Once()
		proc.On("Process", mockCtx, data[0]).Return(data[0], errors.New("some error")).Once()
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		if err := pf.Register("test-processor", newProcessor(proc)); err != nil {
			t.Fatal(err)
		}

		sink := mocks.NewSink()
		sink.On("Init", mockCtx, buildPluginConfig(validRecipe.Sinks[0])).Return(nil).Once()
		sink.On("Close").Return(nil)
		defer sink.AssertExpectations(t)
		sf := registry.NewSinkFactory()
		if err := sf.Register("test-sink", newSink(sink)); err != nil {
			t.Fatal(err)
		}

		monitor := newMockMonitor()
		monitor.On("RecordRun", mock.AnythingOfType("agent.Run")).Once()
		defer monitor.AssertExpectations(t)

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           utils.Logger,
			Monitor:          monitor,
		})
		run := r.Run(ctx, validRecipe)
		assert.False(t, run.Success)
		assert.Error(t, run.Error)
	})

	t.Run("should return error when processing panics", func(t *testing.T) {
		data := []models.Record{
			models.NewRecord(&v1beta2.Asset{}),
		}

		extr := mocks.NewExtractor()
		extr.SetEmit(data)
		extr.On("Init", mockCtx, buildPluginConfig(validRecipe.Source)).Return(nil).Once()
		extr.On("Extract", mockCtx, mock.AnythingOfType("plugins.Emit")).Return(nil).Once()
		ef := registry.NewExtractorFactory()
		if err := ef.Register("test-extractor", newExtractor(extr)); err != nil {
			t.Fatal(err)
		}

		proc := new(panicProcessor)
		proc.On("Init", mockCtx, buildPluginConfig(validRecipe.Processors[0])).Return(nil).Once()
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		if err := pf.Register("test-processor", newProcessor(proc)); err != nil {
			t.Fatal(err)
		}

		sink := mocks.NewSink()
		sink.On("Init", mockCtx, buildPluginConfig(validRecipe.Sinks[0])).Return(nil).Once()
		sink.On("Close").Return(nil)
		defer sink.AssertExpectations(t)
		sf := registry.NewSinkFactory()
		if err := sf.Register("test-sink", newSink(sink)); err != nil {
			t.Fatal(err)
		}

		monitor := newMockMonitor()
		monitor.On("RecordRun", mock.AnythingOfType("agent.Run")).Once()
		defer monitor.AssertExpectations(t)

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           utils.Logger,
			Monitor:          monitor,
		})
		run := r.Run(ctx, validRecipe)
		assert.False(t, run.Success)
		assert.Error(t, run.Error)
	})

	t.Run("should not return error when sink fails", func(t *testing.T) {
		data := []models.Record{
			models.NewRecord(&v1beta2.Asset{}),
		}

		extr := mocks.NewExtractor()
		extr.SetEmit(data)
		extr.On("Init", mockCtx, buildPluginConfig(validRecipe.Source)).Return(nil).Once()
		extr.On("Extract", mockCtx, mock.AnythingOfType("plugins.Emit")).Return(nil)
		ef := registry.NewExtractorFactory()
		if err := ef.Register("test-extractor", newExtractor(extr)); err != nil {
			t.Fatal(err)
		}

		proc := mocks.NewProcessor()
		proc.On("Init", mockCtx, buildPluginConfig(validRecipe.Processors[0])).Return(nil).Once()
		proc.On("Process", mockCtx, data[0]).Return(data[0], nil)
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		if err := pf.Register("test-processor", newProcessor(proc)); err != nil {
			t.Fatal(err)
		}

		sink := mocks.NewSink()
		sink.On("Init", mockCtx, buildPluginConfig(validRecipe.Sinks[0])).Return(nil).Once()
		sink.On("Sink", mockCtx, data).Return(errors.New("some error"))
		sink.On("Close").Return(nil)
		defer sink.AssertExpectations(t)
		sf := registry.NewSinkFactory()
		if err := sf.Register("test-sink", newSink(sink)); err != nil {
			t.Fatal(err)
		}

		monitor := newMockMonitor()
		monitor.On("RecordRun", mock.AnythingOfType("agent.Run")).Once()
		monitor.On("RecordPlugin", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("bool"))
		defer monitor.AssertExpectations(t)

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           utils.Logger,
			Monitor:          monitor,
		})
		run := r.Run(ctx, validRecipe)
		assert.True(t, run.Success)
		assert.NoError(t, run.Error)
	})

	t.Run("should return error when sink fails if StopOnSinkError is true", func(t *testing.T) {
		data := []models.Record{
			models.NewRecord(&v1beta2.Asset{}),
		}

		extr := mocks.NewExtractor()
		extr.SetEmit(data)
		extr.On("Init", mockCtx, buildPluginConfig(validRecipe.Source)).Return(nil).Once()
		extr.On("Extract", mockCtx, mock.AnythingOfType("plugins.Emit")).Return(nil)
		ef := registry.NewExtractorFactory()
		if err := ef.Register("test-extractor", newExtractor(extr)); err != nil {
			t.Fatal(err)
		}

		proc := mocks.NewProcessor()
		proc.On("Init", mockCtx, buildPluginConfig(validRecipe.Processors[0])).Return(nil).Once()
		proc.On("Process", mockCtx, data[0]).Return(data[0], nil)
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		if err := pf.Register("test-processor", newProcessor(proc)); err != nil {
			t.Fatal(err)
		}

		sink := mocks.NewSink()
		sink.On("Init", mockCtx, buildPluginConfig(validRecipe.Sinks[0])).Return(nil).Once()
		sink.On("Sink", mockCtx, data).Return(errors.New("some error"))
		sink.On("Close").Return(nil)
		defer sink.AssertExpectations(t)
		sf := registry.NewSinkFactory()
		if err := sf.Register("test-sink", newSink(sink)); err != nil {
			t.Fatal(err)
		}

		monitor := newMockMonitor()
		monitor.On("RecordRun", mock.AnythingOfType("agent.Run")).Once()
		defer monitor.AssertExpectations(t)

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           utils.Logger,
			StopOnSinkError:  false,
			Monitor:          monitor,
		})

		run := r.Run(ctx, validRecipe)
		assert.False(t, run.Success)
		assert.Error(t, run.Error)
	})

	t.Run("should return error when sink fails to close", func(t *testing.T) {
		data := []models.Record{
			models.NewRecord(&v1beta2.Asset{}),
		}

		extr := mocks.NewExtractor()
		extr.SetEmit(data)
		extr.On("Init", mockCtx, buildPluginConfig(validRecipe.Source)).Return(nil).Once()
		extr.On("Extract", mockCtx, mock.AnythingOfType("plugins.Emit")).Return(nil)
		ef := registry.NewExtractorFactory()
		if err := ef.Register("test-extractor", newExtractor(extr)); err != nil {
			t.Fatal(err)
		}

		proc := mocks.NewProcessor()
		proc.On("Init", mockCtx, buildPluginConfig(validRecipe.Processors[0])).Return(nil).Once()
		proc.On("Process", mockCtx, data[0]).Return(data[0], nil)
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		if err := pf.Register("test-processor", newProcessor(proc)); err != nil {
			t.Fatal(err)
		}

		sink := mocks.NewSink()
		sink.On("Init", mockCtx, buildPluginConfig(validRecipe.Sinks[0])).Return(nil).Once()
		sink.On("Sink", mockCtx, data).Return(nil)
		sink.On("Close").Return(errors.New("some error"))
		defer sink.AssertExpectations(t)
		sf := registry.NewSinkFactory()
		if err := sf.Register("test-sink", newSink(sink)); err != nil {
			t.Fatal(err)
		}

		monitor := newMockMonitor()
		monitor.On("RecordRun", mock.AnythingOfType("agent.Run")).Once()
		defer monitor.AssertExpectations(t)

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           utils.Logger,
			StopOnSinkError:  false,
			Monitor:          monitor,
		})

		run := r.Run(ctx, validRecipe)
		assert.False(t, run.Success)
		assert.Error(t, run.Error)
	})

	t.Run("should return run on success", func(t *testing.T) {
		data := []models.Record{
			models.NewRecord(&v1beta2.Asset{}),
		}

		extr := mocks.NewExtractor()
		extr.SetEmit(data)
		extr.On("Init", mockCtx, buildPluginConfig(validRecipe.Source)).Return(nil).Once()
		extr.On("Extract", mockCtx, mock.AnythingOfType("plugins.Emit")).
			Run(func(mock.Arguments) {
				time.Sleep(2 * time.Millisecond)
			}).
			Return(nil)
		ef := registry.NewExtractorFactory()
		if err := ef.Register("test-extractor", newExtractor(extr)); err != nil {
			t.Fatal(err)
		}

		proc := mocks.NewProcessor()
		proc.On("Init", mockCtx, buildPluginConfig(validRecipe.Processors[0])).Return(nil).Once()
		proc.On("Process", mockCtx, data[0]).
			Run(func(mock.Arguments) {
				time.Sleep(2 * time.Millisecond)
			}).
			Return(data[0], nil)
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		if err := pf.Register("test-processor", newProcessor(proc)); err != nil {
			t.Fatal(err)
		}

		sink := mocks.NewSink()
		sink.On("Init", mockCtx, buildPluginConfig(validRecipe.Sinks[0])).Return(nil).Once()
		sink.On("Sink", mockCtx, data).
			Run(func(mock.Arguments) {
				time.Sleep(2 * time.Millisecond)
			}).
			Return(nil)
		sink.On("Close").Return(nil)
		defer sink.AssertExpectations(t)
		sf := registry.NewSinkFactory()
		if err := sf.Register("test-sink", newSink(sink)); err != nil {
			t.Fatal(err)
		}

		monitor := newMockMonitor()
		monitor.On("RecordRun", mock.AnythingOfType("agent.Run")).Once()
		monitor.On("RecordPlugin", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("bool"))
		defer monitor.AssertExpectations(t)

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           utils.Logger,
			Monitor:          monitor,
		})
		run := r.Run(ctx, validRecipe)
		assert.NoError(t, run.Error)
		assert.NotEmpty(t, run.DurationInMs)
		assert.Equal(t, validRecipe, run.Recipe)
	})

	t.Run("should collect run metrics", func(t *testing.T) {
		expectedDuration := 1000
		data := []models.Record{
			models.NewRecord(&v1beta2.Asset{}),
		}
		timerFn := func() func() int {
			return func() int {
				return expectedDuration
			}
		}

		extr := mocks.NewExtractor()
		extr.SetEmit(data)
		extr.On("Init", mockCtx, buildPluginConfig(validRecipe.Source)).Return(nil).Once()
		extr.On("Extract", mockCtx, mock.AnythingOfType("plugins.Emit")).Return(nil)
		ef := registry.NewExtractorFactory()
		if err := ef.Register("test-extractor", newExtractor(extr)); err != nil {
			t.Fatal(err)
		}

		proc := mocks.NewProcessor()
		proc.On("Init", mockCtx, buildPluginConfig(validRecipe.Processors[0])).Return(nil).Once()
		proc.On("Process", mockCtx, data[0]).Return(data[0], nil)
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		if err := pf.Register("test-processor", newProcessor(proc)); err != nil {
			t.Fatal(err)
		}

		sink := mocks.NewSink()
		sink.On("Init", mockCtx, buildPluginConfig(validRecipe.Sinks[0])).Return(nil).Once()
		sink.On("Sink", mockCtx, data).Return(nil)
		sink.On("Close").Return(nil)
		defer sink.AssertExpectations(t)
		sf := registry.NewSinkFactory()
		if err := sf.Register("test-sink", newSink(sink)); err != nil {
			t.Fatal(err)
		}

		monitor := newMockMonitor()
		monitor.On("RecordRun", mock.AnythingOfType("agent.Run")).Once()
		monitor.On("RecordPlugin", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("bool"))
		defer monitor.AssertExpectations(t)

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Monitor:          monitor,
			Logger:           utils.Logger,
			TimerFn:          timerFn,
		})
		run := r.Run(ctx, validRecipe)
		assert.True(t, run.Success)
		assert.NoError(t, run.Error)
		assert.Equal(t, validRecipe, run.Recipe)
	})

	t.Run("should retry if extractor returns retry error", func(t *testing.T) {
		err := errors.New("some-error")
		data := []models.Record{
			models.NewRecord(&v1beta2.Asset{}),
		}

		extr := mocks.NewExtractor()
		extr.SetEmit(data)
		extr.On("Init", mockCtx, buildPluginConfig(validRecipe.Source)).Return(nil).Once()
		extr.On("Extract", mockCtx, mock.AnythingOfType("plugins.Emit")).Return(plugins.NewRetryError(err)).Twice()
		extr.On("Extract", mockCtx, mock.AnythingOfType("plugins.Emit")).Return(nil)
		ef := registry.NewExtractorFactory()
		if err := ef.Register("test-extractor", newExtractor(extr)); err != nil {
			t.Fatal(err)
		}

		proc := mocks.NewProcessor()
		proc.On("Init", mockCtx, buildPluginConfig(validRecipe.Processors[0])).Return(nil).Once()
		proc.On("Process", mockCtx, data[0]).Return(data[0], nil)
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		if err := pf.Register("test-processor", newProcessor(proc)); err != nil {
			t.Fatal(err)
		}

		sink := mocks.NewSink()
		sink.On("Init", mockCtx, buildPluginConfig(validRecipe.Sinks[0])).Return(nil).Once()
		sink.On("Sink", mockCtx, data).Return(nil)
		sink.On("Close").Return(nil)
		defer sink.AssertExpectations(t)

		sf := registry.NewSinkFactory()
		if err := sf.Register("test-sink", newSink(sink)); err != nil {
			t.Fatal(err)
		}

		monitor := newMockMonitor()
		monitor.On("RecordRun", mock.AnythingOfType("agent.Run")).Once()
		monitor.On("RecordPlugin", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("bool"))
		defer monitor.AssertExpectations(t)

		r := agent.NewAgent(agent.Config{
			ExtractorFactory:     ef,
			ProcessorFactory:     pf,
			SinkFactory:          sf,
			Logger:               utils.Logger,
			Monitor:              monitor,
			MaxRetries:           2,                    // need to retry "at least" 2 times since Extractor returns RetryError twice
			RetryInitialInterval: 1 * time.Millisecond, // this is to override default retry interval to reduce test time
		})
		run := r.Run(ctx, validRecipe)
		assert.NoError(t, run.Error)
		assert.Equal(t, validRecipe, run.Recipe)
	})

	t.Run("should retry if sink returns retry error", func(t *testing.T) {
		err := errors.New("some-error")
		data := []models.Record{
			models.NewRecord(&v1beta2.Asset{}),
		}

		extr := mocks.NewExtractor()
		extr.SetEmit(data)
		extr.On("Init", mockCtx, buildPluginConfig(validRecipe.Source)).Return(nil).Once()
		extr.On("Extract", mockCtx, mock.AnythingOfType("plugins.Emit")).Return(nil)
		ef := registry.NewExtractorFactory()
		if err := ef.Register("test-extractor", newExtractor(extr)); err != nil {
			t.Fatal(err)
		}

		proc := mocks.NewProcessor()
		proc.On("Init", mockCtx, buildPluginConfig(validRecipe.Processors[0])).Return(nil).Once()
		proc.On("Process", mockCtx, data[0]).Return(data[0], nil)
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		if err := pf.Register("test-processor", newProcessor(proc)); err != nil {
			t.Fatal(err)
		}

		sink := mocks.NewSink()
		sink.On("Init", mockCtx, buildPluginConfig(validRecipe.Sinks[0])).Return(nil).Once()
		sink.On("Sink", mockCtx, data).Return(plugins.NewRetryError(err)).Once()
		sink.On("Sink", mockCtx, data).Return(nil)
		sink.On("Close").Return(nil)
		defer sink.AssertExpectations(t)
		sf := registry.NewSinkFactory()
		if err := sf.Register("test-sink", newSink(sink)); err != nil {
			t.Fatal(err)
		}

		monitor := newMockMonitor()
		monitor.On("RecordRun", mock.AnythingOfType("agent.Run")).Once()
		monitor.On("RecordPlugin", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("bool"))
		defer monitor.AssertExpectations(t)

		r := agent.NewAgent(agent.Config{
			ExtractorFactory:     ef,
			ProcessorFactory:     pf,
			SinkFactory:          sf,
			Logger:               utils.Logger,
			Monitor:              monitor,
			MaxRetries:           2,                    // need to retry "at least" 2 times since Sink returns RetryError twice
			RetryInitialInterval: 1 * time.Millisecond, // this is to override default retry interval to reduce test time
		})
		run := r.Run(ctx, validRecipe)
		assert.NoError(t, run.Error)
		assert.Equal(t, validRecipe, run.Recipe)
	})

	t.Run("should respect context cancellation and stop retries", func(t *testing.T) {
		err := errors.New("some-error")
		ctx, cancel := context.WithCancel(ctx)
		data := []models.Record{
			models.NewRecord(&v1beta2.Asset{}),
		}

		extr := mocks.NewExtractor()
		extr.SetEmit(data)
		extr.On("Init", utils.OfTypeContext(), buildPluginConfig(validRecipe.Source)).Return(nil).Once()
		extr.On("Extract", utils.OfTypeContext(), mock.AnythingOfType("plugins.Emit")).Return(nil)
		ef := registry.NewExtractorFactory()
		if err := ef.Register("test-extractor", newExtractor(extr)); err != nil {
			t.Fatal(err)
		}

		proc := mocks.NewProcessor()
		proc.On("Init", utils.OfTypeContext(), buildPluginConfig(validRecipe.Processors[0])).Return(nil).Once()
		proc.On("Process", utils.OfTypeContext(), data[0]).Return(data[0], nil)
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		if err := pf.Register("test-processor", newProcessor(proc)); err != nil {
			t.Fatal(err)
		}

		sink := mocks.NewSink()
		sink.On("Init", utils.OfTypeContext(), buildPluginConfig(validRecipe.Sinks[0])).Return(nil).Once()
		// Sink should not be called more than once in total since we cancel the context after the first call.
		sink.On("Sink", utils.OfTypeContext(), data).Return(plugins.NewRetryError(err)).Once().Run(func(args mock.Arguments) {
			go func() {
				cancel()
			}()
		})
		sink.On("Close").Return(nil)
		defer sink.AssertExpectations(t)
		sf := registry.NewSinkFactory()
		if err := sf.Register("test-sink", newSink(sink)); err != nil {
			t.Fatal(err)
		}

		monitor := newMockMonitor()
		monitor.On("RecordRun", mock.AnythingOfType("agent.Run")).Once()
		monitor.On("RecordPlugin", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("bool"))
		defer monitor.AssertExpectations(t)

		r := agent.NewAgent(agent.Config{
			ExtractorFactory:     ef,
			ProcessorFactory:     pf,
			SinkFactory:          sf,
			Logger:               utils.Logger,
			Monitor:              monitor,
			MaxRetries:           5,
			RetryInitialInterval: 10 * time.Second,
		})
		run := r.Run(ctx, validRecipe)
		assert.NoError(t, run.Error)
		assert.Equal(t, validRecipe, run.Recipe)
	})

	t.Run("should work correctly with multiple processors", func(t *testing.T) {
		var records []models.Record
		sink := mocks.NewSink()
		sink.On("Init", mockCtx, mock.AnythingOfType("plugins.Config")).
			Return(nil)
		sink.On("Sink", mockCtx, mock.Anything).
			Run(func(args mock.Arguments) {
				records = append(records, args.Get(1).([]models.Record)...)
			}).
			Return(nil)
		sink.On("Close").Return(nil)
		sf := registry.NewSinkFactory()
		if err := sf.Register("test-sink", newSink(sink)); err != nil {
			t.Fatal(err)
		}

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: registry.Extractors,
			ProcessorFactory: registry.Processors,
			SinkFactory:      sf,
			Logger:           utils.Logger,
		})
		run := r.Run(ctx, recipe.Recipe{
			Name:    "sample",
			Version: "v1beta1",
			Source: recipe.PluginRecipe{
				Name:  "application_yaml",
				Scope: "application-test",
				Config: map[string]interface{}{
					"file": "../plugins/extractors/application_yaml/testdata/application.detailed.yaml",
				},
			},
			Processors: []recipe.PluginRecipe{
				{
					Name: "script",
					Config: map[string]interface{}{
						"engine": "tengo",
						"script": heredoc.Doc(`
							text := import("text")
							
							for u in asset.lineage.upstreams {
							  u.urn = text.contains(u.urn, "kafka") ? text.replace(u.urn, ".yonkou.io", "", -1) : u.urn
							}
							for u in asset.lineage.downstreams {
							  u.urn = text.contains(u.urn, "kafka") ? text.replace(u.urn, ".company.com", "", -1) : u.urn
							}
						`),
					},
				},
				{
					Name: "labels",
					Config: map[string]interface{}{
						"labels": map[string]string{"field_a": "1"},
					},
				},
			},
			Sinks: []recipe.PluginRecipe{
				{Name: "test-sink", Config: map[string]interface{}{}},
			},
		})
		assert.NoError(t, run.Error)
		expected := &v1beta2.Asset{
			Urn:         "urn:application_yaml:application-test:application:test",
			Name:        "test",
			Service:     "application_yaml",
			Type:        "application",
			Url:         "http://company.com/myteam/test",
			Description: "My incredible project",
			Data: utils.BuildAny(t, &v1beta2.Application{
				Id:         "test-id",
				Version:    "c23sdf6",
				Attributes: &structpb.Struct{},
				CreateTime: ts(t, "2006-01-02T15:04:05Z"),
				UpdateTime: ts(t, "2006-01-02T15:04:05Z"),
			}),
			Owners: []*v1beta2.Owner{{
				Urn:   "123",
				Name:  "myteam",
				Email: "myteam@company.com",
			}},
			Lineage: &v1beta2.Lineage{
				Upstreams: []*v1beta2.Resource{
					{Urn: "urn:bigquery:bq-raw-internal:table:bq-raw-internal:dagstream.production_feast09_s2id13_30min_demand"},
					{Urn: "urn:kafka:int-dagstream-kafka:topic:staging_feast09_s2id13_30min_demand"},
				},
				Downstreams: []*v1beta2.Resource{
					{Urn: "urn:kafka:1-my-kafka,2-my-kafka:topic:staging_feast09_mixed_granularity_demand_forecast_3es"},
				},
			},
			Labels: map[string]string{"field_a": "1", "x": "y"},
		}
		utils.AssertEqualProto(t, expected, records[0].Data())
	})

	t.Run("should close stream after sink finishes writing records", func(t *testing.T) {
		r := agent.NewAgent(agent.Config{
			ExtractorFactory: registry.Extractors,
			ProcessorFactory: registry.Processors,
			SinkFactory:      registry.Sinks,
			Logger:           utils.Logger,
			StopOnSinkError:  true,
		})

		run := r.Run(ctx, recipe.Recipe{
			Name:    "sink_close-test",
			Version: "v1beta1",
			Source: recipe.PluginRecipe{
				Name:  "application_yaml",
				Scope: "application-test",
				Config: map[string]interface{}{
					"file": "../plugins/extractors/application_yaml/testdata/application.detailed.yaml",
				},
			},
			Sinks: []recipe.PluginRecipe{{
				Name: "file",
				Config: map[string]interface{}{
					"path":      "./application_yaml-sink[yaml].out",
					"format":    "yaml",
					"overwrite": true,
				},
			}},
		})
		assert.NoError(t, run.Error)
	})
}

func TestAgentRunMultiple(t *testing.T) {
	t.Run("should return list of runs when finished", func(t *testing.T) {
		validRecipe2 := validRecipe
		validRecipe2.Name = "sample-2"
		recipeList := []recipe.Recipe{validRecipe, validRecipe2}
		data := []models.Record{
			models.NewRecord(&v1beta2.Asset{}),
		}
		extr := mocks.NewExtractor()
		extr.SetEmit(data)
		extr.On("Init", mockCtx, buildPluginConfig(validRecipe.Source)).Return(nil)
		extr.On("Extract", mockCtx, mock.AnythingOfType("plugins.Emit")).Return(nil)
		ef := registry.NewExtractorFactory()
		if err := ef.Register("test-extractor", newExtractor(extr)); err != nil {
			t.Fatal(err)
		}

		proc := mocks.NewProcessor()
		proc.On("Init", mockCtx, buildPluginConfig(validRecipe.Processors[0])).Return(nil)
		proc.On("Process", mockCtx, data[0]).Return(data[0], nil)
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		if err := pf.Register("test-processor", newProcessor(proc)); err != nil {
			t.Fatal(err)
		}

		sink := mocks.NewSink()
		sink.On("Init", mockCtx, buildPluginConfig(validRecipe.Sinks[0])).Return(nil)
		sink.On("Sink", mockCtx, data).Return(nil)
		sink.On("Close").Return(nil)
		defer sink.AssertExpectations(t)
		sf := registry.NewSinkFactory()
		if err := sf.Register("test-sink", newSink(sink)); err != nil {
			t.Fatal(err)
		}

		monitor := newMockMonitor()
		monitor.On("RecordRun", mock.AnythingOfType("agent.Run"))
		monitor.On("RecordPlugin", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("bool"))
		defer monitor.AssertExpectations(t)

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           utils.Logger,
			Monitor:          monitor,
		})
		runs := r.RunMultiple(ctx, recipeList)

		assert.Len(t, runs, len(recipeList))
		for i := range runs {
			runs[i].DurationInMs = 0
		}
		assert.Equal(t, []agent.Run{
			{Recipe: validRecipe, RecordCount: len(data), Success: true},
			{Recipe: validRecipe2, RecordCount: len(data), Success: true},
		}, runs)
	})
}

func TestValidate(t *testing.T) {
	t.Run("should return error if plugins in recipe not found in Factory", func(t *testing.T) {
		r := agent.NewAgent(agent.Config{
			ExtractorFactory: registry.NewExtractorFactory(),
			ProcessorFactory: registry.NewProcessorFactory(),
			SinkFactory:      registry.NewSinkFactory(),
			Logger:           utils.Logger,
		})
		var expectedErrs []error
		errs := r.Validate(validRecipe)
		expectedErrs = append(expectedErrs, plugins.NotFoundError{Type: plugins.PluginTypeExtractor, Name: "test-extractor"})
		expectedErrs = append(expectedErrs, plugins.NotFoundError{Type: plugins.PluginTypeSink, Name: "test-sink"})
		expectedErrs = append(expectedErrs, plugins.NotFoundError{Type: plugins.PluginTypeProcessor, Name: "test-processor"})
		assert.Equal(t, 3, len(errs))
		assert.Equal(t, expectedErrs, errs)
	})
	t.Run("", func(t *testing.T) {
		var invalidRecipe = recipe.Recipe{
			Name: "sample",
			Source: recipe.PluginRecipe{
				Name: "test-extractor",
				Config: map[string]interface{}{
					"proc-foo": "proc-bar",
				},
			},
			Processors: []recipe.PluginRecipe{
				{
					Name: "test-processor",
					Config: map[string]interface{}{
						"proc-foo": "proc-bar",
					},
				},
			},
			Sinks: []recipe.PluginRecipe{
				{
					Name: "test-sink",
					Config: map[string]interface{}{
						"url": "http://localhost:3000/data",
					},
				},
			},
		}

		extr := mocks.NewExtractor()
		err := plugins.InvalidConfigError{}
		extr.On("Validate", buildPluginConfig(invalidRecipe.Source)).Return(err).Once()
		ef := registry.NewExtractorFactory()
		if err := ef.Register("test-extractor", newExtractor(extr)); err != nil {
			t.Fatal(err)
		}

		proc := mocks.NewProcessor()
		proc.On("Validate", buildPluginConfig(invalidRecipe.Processors[0])).Return(err).Once()
		defer proc.AssertExpectations(t)
		pf := registry.NewProcessorFactory()
		if err := pf.Register("test-processor", newProcessor(proc)); err != nil {
			t.Fatal(err)
		}

		sink := mocks.NewSink()
		sink.On("Validate", buildPluginConfig(invalidRecipe.Sinks[0])).Return(err).Once()
		defer sink.AssertExpectations(t)
		sf := registry.NewSinkFactory()
		if err := sf.Register("test-sink", newSink(sink)); err != nil {
			t.Fatal(err)
		}

		r := agent.NewAgent(agent.Config{
			ExtractorFactory: ef,
			ProcessorFactory: pf,
			SinkFactory:      sf,
			Logger:           utils.Logger,
			Monitor:          newMockMonitor(),
		})

		var expectedErrs []error
		errs := r.Validate(invalidRecipe)
		assert.Equal(t, 3, len(errs))
		expectedErrs = append(expectedErrs, enrichInvalidConfigError(err, invalidRecipe.Source.Name, plugins.PluginTypeExtractor))
		expectedErrs = append(expectedErrs, enrichInvalidConfigError(err, invalidRecipe.Sinks[0].Name, plugins.PluginTypeSink))
		expectedErrs = append(expectedErrs, enrichInvalidConfigError(err, invalidRecipe.Processors[0].Name, plugins.PluginTypeProcessor))
		assert.Equal(t, expectedErrs, errs)
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

func (m *mockMonitor) RecordPlugin(recipeName, pluginName, pluginType string, success bool) {
	m.Called(recipeName, pluginName, pluginType, success)
}

type panicExtractor struct {
	mocks.Extractor
}

func (e *panicExtractor) Extract(_ context.Context, _ plugins.Emit) (err error) {
	panic("panicking")
}

type panicProcessor struct {
	mocks.Processor
}

func (p *panicProcessor) Process(_ context.Context, _ models.Record) (dst models.Record, err error) {
	panic("panicking")
}

// enrichInvalidConfigError enrich the error with plugin information
func enrichInvalidConfigError(err error, pluginName string, pluginType plugins.PluginType) error {
	if errors.As(err, &plugins.InvalidConfigError{}) {
		icErr := err.(plugins.InvalidConfigError)
		icErr.PluginName = pluginName
		icErr.Type = pluginType

		return icErr
	}

	return err
}

func buildPluginConfig(pr recipe.PluginRecipe) plugins.Config {
	return plugins.Config{RawConfig: pr.Config, URNScope: pr.Scope}
}

func ts(t *testing.T, s string) *timestamppb.Timestamp {
	ts, err := time.Parse(time.RFC3339, s)
	require.NoError(t, err)
	return timestamppb.New(ts)
}
