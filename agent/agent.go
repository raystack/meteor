package agent

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/odpf/meteor/recipe"
	"github.com/odpf/meteor/registry"
)

type Agent struct {
	extractorFactory *registry.ExtractorFactory
	processorFactory *registry.ProcessorFactory
	sinkFactory      *registry.SinkFactory
	monitor          Monitor
}

func NewAgent(ef *registry.ExtractorFactory, pf *registry.ProcessorFactory, sf *registry.SinkFactory, mt Monitor) *Agent {
	if isNilMonitor(mt) {
		mt = new(defaultMonitor)
	}
	return &Agent{
		extractorFactory: ef,
		processorFactory: pf,
		sinkFactory:      sf,
		monitor:          mt,
	}
}

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

func (r *Agent) Run(recipe recipe.Recipe) (run Run) {
	var wg sync.WaitGroup
	var (
		getDuration = r.startDuration()
		channel     = make(chan interface{})
	)
	run.Recipe = recipe

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// run extractors
	extrChannel := channel
	go func() {
		err := r.runExtractor(ctx, recipe.Source, extrChannel)
		if err != nil {
			run.Error = r.buildTaskError(TaskTypeExtract, recipe.Source.Type, err)
		}

		close(extrChannel)
	}()

	// run processors
	for _, processorRecipe := range recipe.Processors {
		inChannel := channel
		outChannel := make(chan interface{})

		// need to store the recipe since it would be needed inside a goroutine
		// not storing it inside the loop scope would cause
		// processorRecipe to always be the last recipe in the loop
		tempRecipe := processorRecipe
		go func() {
			err := r.runProcessor(ctx, tempRecipe, inChannel, outChannel)
			if err != nil {
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
			err := r.runSink(ctx, tempRecipe, channel)
			if err != nil {
				run.Error = r.buildTaskError(TaskTypeSink, tempRecipe.Name, err)
			}
			wg.Done()
		}()
	}

	wg.Wait()

	success := run.Error == nil
	r.monitor.RecordRun(recipe, getDuration(), success)

	return
}

func (r *Agent) runExtractor(ctx context.Context, sourceRecipe recipe.SourceRecipe, in chan<- interface{}) (err error) {
	extractor, err := r.extractorFactory.Get(sourceRecipe.Type)
	if err != nil {
		return
	}
	err = extractor.Extract(ctx, sourceRecipe.Config, in)
	if err != nil {
		return
	}

	return
}

func (r *Agent) runProcessor(ctx context.Context, processorRecipe recipe.ProcessorRecipe, in <-chan interface{}, out chan<- interface{}) (err error) {
	processor, err := r.processorFactory.Get(processorRecipe.Name)
	if err != nil {
		return
	}
	err = processor.Process(ctx, processorRecipe.Config, in, out)
	if err != nil {
		return
	}

	return
}

func (r *Agent) runSink(ctx context.Context, sinkRecipe recipe.SinkRecipe, in <-chan interface{}) (err error) {
	sink, err := r.sinkFactory.Get(sinkRecipe.Name)
	if err != nil {
		return
	}
	err = sink.Sink(ctx, sinkRecipe.Config, in)
	if err != nil {
		return
	}

	return
}

func (r *Agent) buildTaskError(taskType TaskType, name string, err error) error {
	return fmt.Errorf(
		"error running %s task \"%s\": %s",
		taskType,
		name,
		err)
}

func (r *Agent) startDuration() func() int {
	start := time.Now()
	return func() int {
		return int(time.Since(start).Milliseconds())
	}
}
