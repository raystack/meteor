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
		stream      = newStream()
	)

	runExtractor, err := r.setupExtractor(ctx, recipe.Source, stream)
	if err != nil {
		run.Error = err
		return
	}

	for _, pr := range recipe.Processors {
		if err := r.setupProcessor(ctx, pr, stream); err != nil {
			run.Error = err
			return
		}
	}

	for _, sr := range recipe.Sinks {
		if err := r.setupSink(ctx, sr, stream); err != nil {
			run.Error = err
			return
		}
	}

	// create a goroutine to let extractor concurrently emit data
	// while stream is listening via stream.Listen().
	go func() {
		err = runExtractor()
		if err != nil {
			run.Error = err
		}
		stream.Close()
	}()

	// start listening.
	// this process is blocking
	if err := stream.broadcast(); err != nil {
		run.Error = err
	}

	// code will reach here stream.Listen() is done.
	success := run.Error == nil
	durationInMs := getDuration()
	r.monitor.RecordRun(recipe, durationInMs, success)
	run.DurationInMs = durationInMs

	if success {
		r.logger.Info("done running recipe", "recipe", recipe.Name, "duration_ms", durationInMs)
	} else {
		r.logger.Error("error running recipe", "recipe", recipe.Name, "duration_ms", durationInMs, "err", run.Error)
	}

	return
}

func (r *Agent) setupExtractor(ctx context.Context, sr recipe.SourceRecipe, str *stream) (runFn func() error, err error) {
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
		err = extractor.Extract(ctx, str.push)
		if err != nil {
			err = errors.Wrapf(err, "error running extractor \"%s\"", sr.Type)
		}

		return
	}
	return
}

func (r *Agent) setupProcessor(ctx context.Context, pr recipe.ProcessorRecipe, str *stream) (err error) {
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

	str.setMiddleware(func(src models.Record) (dst models.Record, err error) {
		dst, err = proc.Process(ctx, src)
		if err != nil {
			err = errors.Wrapf(err, "error running processor \"%s\"", pr.Name)
			return
		}

		return
	})

	return
}

func (r *Agent) setupSink(ctx context.Context, sr recipe.SinkRecipe, stream *stream) (err error) {
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

	stream.subscribe(func(records []models.Record) (err error) {
		err = sink.Sink(ctx, records)
		if err != nil {
			err = errors.Wrapf(err, "error running sink \"%s\"", sr.Name)
			return
		}

		return
	}, 0)

	return
}

// startDuration starts a timer.
func (r *Agent) startDuration() func() int {
	start := time.Now()
	return func() int {
		return int(time.Since(start).Milliseconds())
	}
}
