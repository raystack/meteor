package agent

import (
	"context"
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
	run.Recipe = recipe
	r.logger.Info("running recipe", "recipe", run.Recipe.Name)

	var (
		ctx         = context.Background()
		getDuration = r.startDuration()
		emitter     = NewEmitter()
	)

	runExtractor, err := r.initExtractor(ctx, recipe.Source, emitter)
	if err != nil {
		run.Error = err
		return
	}
	if err = r.initProcessors(ctx, recipe.Processors, emitter); err != nil {
		run.Error = err
		return
	}
	if err = r.initSinks(ctx, recipe.Sinks, emitter); err != nil {
		run.Error = err
		return
	}

	// create a goroutine to let extractor concurrently emit data
	// while emitter is listening via emitter.Listen().
	go func() {
		err = runExtractor()
		if err != nil {
			run.Error = err
		}
		emitter.Close()
	}()

	// start listening.
	// this process is blocking
	if err := emitter.Listen(); err != nil {
		run.Error = err
	}

	// code will reach here emitter.Listen() is done.
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

func (r *Agent) initExtractor(ctx context.Context, sr recipe.SourceRecipe, emitter *Emitter) (runFn func() error, err error) {
	extractor, err := r.extractorFactory.Get(sr.Type)
	if err != nil {
		err = errors.Wrapf(err, "could not find extractor \"%s\"", sr.Type)
		return
	}
	err = extractor.Init(ctx, sr.Config)
	if err != nil {
		err = errors.Wrapf(err, "could not initiate extractor \"%s\"", sr.Type)
		return
	}

	runFn = func() (err error) {
		err = extractor.Extract(ctx, emitter)
		if err != nil {
			err = errors.Wrapf(err, "error running extractor \"%s\"", sr.Type)
		}

		return
	}
	return
}

func (r *Agent) initProcessors(ctx context.Context, prs []recipe.ProcessorRecipe, emitter *Emitter) (err error) {
	for _, pr := range prs {
		var proc plugins.Processor
		proc, err = r.processorFactory.Get(pr.Name)
		if err != nil {
			err = errors.Wrapf(err, "could not find processor \"%s\"", pr.Name)
			return
		}
		err = proc.Init(ctx, pr.Config)
		if err != nil {
			err = errors.Wrapf(err, "could not initiate processor \"%s\"", pr.Name)
			return
		}

		emitter.SetMiddleware(r.runProcessor(ctx, proc, pr.Name))
	}

	return
}

func (r *Agent) initSinks(ctx context.Context, srs []recipe.SinkRecipe, emitter *Emitter) (err error) {
	for _, sr := range srs {
		var sink plugins.Syncer
		sink, err = r.sinkFactory.Get(sr.Name)
		if err != nil {
			err = errors.Wrapf(err, "could not find sink \"%s\"", sr.Name)
			return
		}
		err = sink.Init(ctx, sr.Config)
		if err != nil {
			err = errors.Wrapf(err, "could not initiate sink \"%s\"", sr.Name)
			return
		}

		emitter.SetListener(r.runSink(ctx, sink, sr.Name), 0)
	}

	return
}

func (r *Agent) runProcessor(ctx context.Context, processor plugins.Processor, name string) EmitterMiddleware {
	return func(src models.Record) (dst models.Record, err error) {
		dst, err = processor.Process(ctx, src)
		if err != nil {
			err = errors.Wrapf(err, "error running processor \"%s\"", name)
			return
		}

		return
	}
}

func (r *Agent) runSink(ctx context.Context, sink plugins.Syncer, name string) func(records []models.Record) error {
	return func(records []models.Record) (err error) {
		err = sink.Sink(ctx, records)
		if err != nil {
			err = errors.Wrapf(err, "error running sink \"%s\"", name)
			return
		}

		return
	}
}

// startDuration starts a timer.
func (r *Agent) startDuration() func() int {
	start := time.Now()
	return func() int {
		return int(time.Since(start).Milliseconds())
	}
}
