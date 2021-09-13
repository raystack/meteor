package agent

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/recipe"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
)

// Agent runs recipes for specified plugins.
type Agent struct {
	extractorFactory *registry.ExtractorFactory
	processorFactory *registry.ProcessorFactory
	sinkFactory      *registry.SinkFactory
	monitor          Monitor
	logger           log.Logger
}

// NewAgent returns an Agent with plugin factories.
func NewAgent(ef *registry.ExtractorFactory, pf *registry.ProcessorFactory, sf *registry.SinkFactory, mt Monitor, logger log.Logger) *Agent {
	if isNilMonitor(mt) {
		mt = new(defaultMonitor)
	}
	return &Agent{
		extractorFactory: ef,
		processorFactory: pf,
		sinkFactory:      sf,
		monitor:          mt,
		logger:           logger,
	}
}

// Validate checks the recipe for linting errors.
func (r *Agent) Validate(rcp recipe.Recipe) (errs []error) {
	if ext, err := r.extractorFactory.Get(rcp.Source.Type); err != nil {
		errs = append(errs, errors.Wrapf(err, "invalid config for %s (%s)", rcp.Source.Type, plugins.PluginTypeExtractor))
	} else {
		if err = ext.Validate(rcp.Source.Config); err != nil {
			errs = append(errs, errors.Wrapf(err, "invalid config for %s (%s)", rcp.Source.Type, plugins.PluginTypeExtractor))
		}
	}

	for _, s := range rcp.Sinks {
		sink, err := r.sinkFactory.Get(s.Name)
		if err != nil {
			errs = append(errs, errors.Wrapf(err, "invalid config for %s (%s)", rcp.Source.Type, plugins.PluginTypeExtractor))
			continue
		}
		if err = sink.Validate(s.Config); err != nil {
			errs = append(errs, errors.Wrapf(err, "invalid config for %s (%s)", s.Name, plugins.PluginTypeSink))
		}
	}

	for _, p := range rcp.Processors {
		procc, err := r.processorFactory.Get(p.Name)
		if err != nil {
			errs = append(errs, errors.Wrapf(err, "invalid config for %s (%s)", rcp.Source.Type, plugins.PluginTypeExtractor))
			continue
		}
		if err = procc.Validate(p.Config); err != nil {
			errs = append(errs, errors.Wrapf(err, "invalid config for %s (%s)", p.Name, plugins.PluginTypeProcessor))
		}
	}
	return
}

// RunMultiple executes multiple recipes.
func (r *Agent) RunMultiple(recipes []recipe.Recipe) []Run {
	var wg sync.WaitGroup
	runs := make([]Run, len(recipes))

	for i, recipe := range recipes {
		wg.Add(1)

		tempIndex := i
		tempRecipe := recipe
		go func() {
			run := r.Run(tempRecipe)
			runs[tempIndex] = run
			wg.Done()
		}()
	}

	wg.Wait()

	return runs
}

// Run executes the specified recipe.
func (r *Agent) Run(recipe recipe.Recipe) (run Run) {
	r.logger.Info("running recipe", "recipe", recipe.Name)
	var wg sync.WaitGroup
	var (
		getDuration = r.startDuration()
		channel     = make(chan models.Record)
	)
	run.Recipe = recipe

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// run extractors
	extrChannel := channel
	go func() {
		if err := r.runExtractor(ctx, recipe.Source, extrChannel); err != nil {
			run.Error = r.buildTaskError(TaskTypeExtract, recipe.Source.Type, err)
		}

		close(extrChannel)
	}()

	// run processors
	for _, processorRecipe := range recipe.Processors {
		inChannel := channel
		outChannel := make(chan models.Record)

		// need to store the recipe since it would be needed inside a goroutine
		// not storing it inside the loop scope would cause
		// processorRecipe to always be the last recipe in the loop
		tempRecipe := processorRecipe
		go func() {
			if err := r.runProcessor(ctx, tempRecipe, inChannel, outChannel); err != nil {
				run.Error = r.buildTaskError(TaskTypeProcess, tempRecipe.Name, err)
			}

			close(outChannel)
		}()

		// replace the channel with the new out channel
		// this would allow the next processor or sink to
		// receive the processed data instead of data directly from extractor
		channel = outChannel
	}

	// run sinks
	for _, sinkRecipe := range recipe.Sinks {
		// need to store the recipe since it would be needed inside a goroutine
		// not storing it inside the loop scope would cause
		// sinkRecipe to always be the last recipe in the loop
		tempRecipe := sinkRecipe
		wg.Add(1)
		go func() {
			if err := r.runSink(ctx, tempRecipe, channel); err != nil {
				run.Error = r.buildTaskError(TaskTypeSink, tempRecipe.Name, err)
			}
			wg.Done()
		}()
	}

	wg.Wait()

	success := run.Error == nil
	durationInMs := getDuration()
	r.monitor.RecordRun(recipe, durationInMs, success)

	if success {
		r.logger.Info("done running recipe", "recipe", recipe.Name, "duration_ms", durationInMs)
	} else {
		r.logger.Error("error running recipe", "recipe", recipe.Name, "duration_ms", durationInMs, "err", run.Error)
	}

	return
}

// runExtractor runs an extractor.
func (r *Agent) runExtractor(ctx context.Context, sourceRecipe recipe.SourceRecipe, in chan<- models.Record) (err error) {
	extractor, err := r.extractorFactory.Get(sourceRecipe.Type)
	if err != nil {
		return
	}
	if err = extractor.Extract(ctx, sourceRecipe.Config, in); err != nil {
		return
	}

	return
}

// runProcessor runs a processor.
func (r *Agent) runProcessor(ctx context.Context, processorRecipe recipe.ProcessorRecipe, in <-chan models.Record, out chan<- models.Record) (err error) {
	processor, err := r.processorFactory.Get(processorRecipe.Name)
	if err != nil {
		return
	}
	if err = processor.Process(ctx, processorRecipe.Config, in, out); err != nil {
		return
	}

	return
}

// runSink runs a sink.
func (r *Agent) runSink(ctx context.Context, sinkRecipe recipe.SinkRecipe, in <-chan models.Record) (err error) {
	sink, err := r.sinkFactory.Get(sinkRecipe.Name)
	if err != nil {
		return
	}
	if err = sink.Sink(ctx, sinkRecipe.Config, in); err != nil {
		return
	}

	return
}

// buildTaskError builds a task error.
func (r *Agent) buildTaskError(taskType TaskType, name string, err error) error {
	return fmt.Errorf(
		"error running %s task \"%s\": %s",
		taskType,
		name,
		err)
}

// startDuration starts a timer.
func (r *Agent) startDuration() func() int {
	start := time.Now()
	return func() int {
		return int(time.Since(start).Milliseconds())
	}
}
