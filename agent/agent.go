package agent

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/raystack/meteor/models"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/recipe"
	"github.com/raystack/meteor/registry"
	"github.com/raystack/salt/log"
)

const defaultBatchSize = 1

// TimerFn of function type
type TimerFn func() func() int

// Agent runs recipes for specified plugins.
type Agent struct {
	extractorFactory *registry.ExtractorFactory
	processorFactory *registry.ProcessorFactory
	sinkFactory      *registry.SinkFactory
	monitor          Monitor
	logger           log.Logger
	retrier          *retrier
	stopOnSinkError  bool
	timerFn          TimerFn
}

// NewAgent returns an Agent with plugin factories.
func NewAgent(config Config) *Agent {
	mt := config.Monitor
	if isNilMonitor(mt) {
		mt = new(defaultMonitor)
	}

	timerFn := config.TimerFn
	if timerFn == nil {
		timerFn = startDuration
	}

	retrier := newRetrier(config.MaxRetries, config.RetryInitialInterval)
	return &Agent{
		extractorFactory: config.ExtractorFactory,
		processorFactory: config.ProcessorFactory,
		sinkFactory:      config.SinkFactory,
		stopOnSinkError:  config.StopOnSinkError,
		monitor:          mt,
		logger:           config.Logger,
		retrier:          retrier,
		timerFn:          timerFn,
	}
}

// Validate checks the recipe for linting errors.
func (r *Agent) Validate(rcp recipe.Recipe) (errs []error) {
	if ext, err := r.extractorFactory.Get(rcp.Source.Name); err != nil {
		errs = append(errs, err)
	} else {
		if err = ext.Validate(plugins.Config{
			URNScope:  rcp.Source.Scope,
			RawConfig: rcp.Source.Config,
		}); err != nil {
			errs = append(errs, r.enrichInvalidConfigError(err, rcp.Source.Name, plugins.PluginTypeExtractor))
		}
	}

	for _, s := range rcp.Sinks {
		sink, err := r.sinkFactory.Get(s.Name)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err = sink.Validate(plugins.Config{RawConfig: s.Config}); err != nil {
			errs = append(errs, r.enrichInvalidConfigError(err, s.Name, plugins.PluginTypeSink))
		}
	}

	for _, p := range rcp.Processors {
		procc, err := r.processorFactory.Get(p.Name)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err = procc.Validate(plugins.Config{RawConfig: p.Config}); err != nil {
			errs = append(errs, r.enrichInvalidConfigError(err, p.Name, plugins.PluginTypeProcessor))
		}
	}
	return
}

// RunMultiple executes multiple recipes.
func (r *Agent) RunMultiple(ctx context.Context, recipes []recipe.Recipe) []Run {
	var wg sync.WaitGroup
	runs := make([]Run, len(recipes))

	wg.Add(len(recipes))
	for i, rcp := range recipes {
		go func(i int, rcp recipe.Recipe) {
			run := r.Run(ctx, rcp)
			runs[i] = run
			wg.Done()
		}(i, rcp)
	}

	wg.Wait()

	return runs
}

// Run executes the specified recipe.
func (r *Agent) Run(ctx context.Context, recipe recipe.Recipe) (run Run) {
	run.Recipe = recipe
	r.logger.Info("running recipe", "recipe", run.Recipe.Name)

	var (
		getDuration = r.timerFn()
		stream      = newStream()
		recordCnt   int64
	)

	defer func() {
		run.DurationInMs = getDuration()
		r.logAndRecordMetrics(run)
	}()

	runExtractor, err := r.setupExtractor(ctx, recipe.Source, stream)
	if err != nil {
		run.Error = errors.Wrap(err, "failed to setup extractor")
		return
	}

	for _, pr := range recipe.Processors {
		if err := r.setupProcessor(ctx, pr, stream); err != nil {
			run.Error = errors.Wrap(err, "failed to setup processor")
			return
		}
	}

	for _, sr := range recipe.Sinks {
		err := r.setupSink(ctx, sr, stream, recipe)
		if err != nil {
			run.Error = errors.Wrap(err, "failed to setup sink")
			return
		}
	}

	// to gather total number of records extracted
	stream.setMiddleware(func(src models.Record) (models.Record, error) {
		atomic.AddInt64(&recordCnt, 1)
		r.logger.Info("Successfully extracted record", "record", src.Data().Urn, "recipe", recipe.Name)
		return src, nil
	})

	// a goroutine to shut down stream gracefully
	go func() {
		<-ctx.Done()
		r.logger.Info("force closing run", "recipe", recipe.Name)
		stream.Close()
	}()

	// a goroutine to let extractor concurrently emit data
	// while stream is listening via stream.Listen().
	go func() {
		defer func() {
			if rcvr := recover(); rcvr != nil {
				r.logger.Error("panic recovered")
				r.logger.Info(string(debug.Stack()))
				run.Error = fmt.Errorf("agent run: close stream: panic: %s", rcvr)
			}
			stream.Shutdown()
		}()
		if err := runExtractor(); err != nil {
			run.Error = errors.Wrap(err, "failed to run extractor")
		}
	}()
	defer stream.Close()

	// start listening.
	// this process is blocking
	if err := stream.broadcast(); err != nil {
		run.Error = errors.Wrap(err, "failed to broadcast stream")
	}

	// code will reach here stream.Listen() is done.
	run.RecordCount = (int)(recordCnt)
	run.Success = run.Error == nil
	return
}

func (r *Agent) setupExtractor(ctx context.Context, sr recipe.PluginRecipe, str *stream) (runFn func() error, err error) {
	extractor, err := r.extractorFactory.Get(sr.Name)
	if err != nil {
		return nil, errors.Wrapf(err, "could not find extractor \"%s\"", sr.Name)
	}
	if err := extractor.Init(ctx, recipeToPluginConfig(sr)); err != nil {
		return nil, errors.Wrapf(err, "could not initiate extractor \"%s\"", sr.Name)
	}

	return func() error {
		if err := extractor.Extract(ctx, str.push); err != nil {
			return errors.Wrapf(err, "error running extractor \"%s\"", sr.Name)
		}
		return nil
	}, nil
}

func (r *Agent) setupProcessor(ctx context.Context, pr recipe.PluginRecipe, str *stream) error {
	proc, err := r.processorFactory.Get(pr.Name)
	if err != nil {
		return errors.Wrapf(err, "could not find processor \"%s\"", pr.Name)
	}
	if err := proc.Init(ctx, recipeToPluginConfig(pr)); err != nil {
		return errors.Wrapf(err, "could not initiate processor \"%s\"", pr.Name)
	}

	str.setMiddleware(func(src models.Record) (models.Record, error) {
		dst, err := proc.Process(ctx, src)
		if err != nil {
			return models.Record{}, errors.Wrapf(err, "error running processor \"%s\"", pr.Name)
		}

		return dst, nil
	})

	return nil
}

func (r *Agent) setupSink(ctx context.Context, sr recipe.PluginRecipe, stream *stream, recipe recipe.Recipe) error {
	sink, err := r.sinkFactory.Get(sr.Name)
	if err != nil {
		return errors.Wrapf(err, "could not find sink \"%s\"", sr.Name)
	}
	if err := sink.Init(ctx, recipeToPluginConfig(sr)); err != nil {
		return errors.Wrapf(err, "could not initiate sink \"%s\"", sr.Name)
	}

	retryNotification := func(e error, d time.Duration) {
		r.logger.Warn(
			fmt.Sprintf("retrying sink in %s", d),
			"retry_delay_ms", d.Milliseconds(),
			"sink", sr.Name,
			"error", e.Error(),
		)
	}
	stream.subscribe(func(records []models.Record) error {
		err := r.retrier.retry(
			ctx,
			func() error { return sink.Sink(ctx, records) },
			retryNotification,
		)

		success := err == nil
		r.monitor.RecordPlugin(recipe.Name, sr.Name, "sink", success)
		if err != nil {
			// once it reaches here, it means that the retry has been exhausted and still got error
			r.logger.Error("error running sink", "sink", sr.Name, "error", err.Error())
			if r.stopOnSinkError {
				return err
			}
			return nil
		}

		r.logger.Info("Successfully published record", "sink", sr.Name, "recipe", recipe.Name)
		return nil
	}, defaultBatchSize)

	stream.onClose(func() {
		if err := sink.Close(); err != nil {
			r.logger.Warn("error closing sink", "sink", sr.Name, "error", err)
		}
	})

	return nil
}

func (r *Agent) logAndRecordMetrics(run Run) {
	r.monitor.RecordRun(run)
	if run.Success {
		r.logger.Info("done running recipe",
			"recipe", run.Recipe.Name,
			"duration_ms", run.DurationInMs,
			"record_count", run.RecordCount)
	} else {
		r.logger.Error("error running recipe",
			"recipe", run.Recipe.Name,
			"duration_ms", run.DurationInMs,
			"records_count", run.RecordCount,
			"err", run.Error)
	}
}

// enrichInvalidConfigError enrich the error with plugin information
func (r *Agent) enrichInvalidConfigError(err error, pluginName string, pluginType plugins.PluginType) error {
	var e plugins.InvalidConfigError
	if !errors.As(err, &e) {
		return err
	}

	e.PluginName = pluginName
	e.Type = pluginType
	return e
}

// startDuration starts a timer.
func startDuration() func() int {
	start := time.Now()
	return func() int {
		duration := time.Since(start).Milliseconds()
		return int(duration)
	}
}
