package recipe

import (
	"context"
	"go.uber.org/multierr"
	"sync"
	"time"

	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/core/processor"
	sinks "github.com/odpf/meteor/core/sink"
)

type Runner struct {
	extractorFactory        *extractor.Factory
	processorFactory *processor.Factory
	sinkFactory      *sinks.Factory
	monitor          Monitor

	wg *sync.WaitGroup
	errChan chan error
}

func NewRunner(
	extractorFactory *extractor.Factory,
	processorFactory *processor.Factory,
	sinkFactory *sinks.Factory,
	monitor Monitor) *Runner {
	if isNilMonitor(monitor) {
		monitor = new(defaultMonitor)
	}
	return &Runner{
		extractorFactory:        extractorFactory,
		processorFactory: processorFactory,
		sinkFactory:      sinkFactory,
		monitor:          monitor,
	}
}

func (r *Runner) Run(recipe Recipe) (err error) {
	r.wg = new(sync.WaitGroup)
	r.errChan = make(chan error)
	getDuration := r.startDuration()
	success := true

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// run all sinks
	var sinkOuts []chan interface{}
	for _, sink := range recipe.Sinks {
		task := &Task{
			Type:   TaskTypeSink,
			Status: TaskStatusReady,
			Name:   sink.Name,
			Config: sink.Config,
		}
		r.wg.Add(1)
		currentSink := make(chan interface{})
		sinkOuts = append(sinkOuts, currentSink)

		go r.runSink(ctx, task, currentSink)
	}

	// run extract
	extractOut := make(chan interface{})
	extractTask := &Task{
		Type:   TaskTypeExtract,
		Status: TaskStatusReady,
		Name:   recipe.Source.Type,
		Config: recipe.Source.Config,
	}
	r.wg.Add(1)
	go r.runExtractor(ctx, extractTask, extractOut)

	// run processors
	processIn := extractOut
	var processorOuts []chan interface{}
	for _, processor := range recipe.Processors {
		r.wg.Add(1)
		task := &Task{
			Type:   TaskTypeProcess,
			Status: TaskStatusReady,
			Name:   processor.Name,
			Config: processor.Config,
		}
		processOut := make(chan interface{})
		processorOuts = append(processorOuts, processOut)
		go r.runProcessor(ctx, task, processIn, processOut)
		processIn = processOut
	}

	go func() {
		// fan out service for sinks
		for val := range processIn {
			for _, so := range sinkOuts {
				so <- val
			}
		}

		// close all fanned-out syncer
		for _, so := range sinkOuts {
			close(so)
		}
	}()

	// err accumulator
	errWg := new(sync.WaitGroup)
	var errs []error
	errWg.Add(1)
	go func() {
		for err := range r.errChan{
			errs = append(errs, err)

			// its safe to call cancel multiple times
			cancel()
		}
		errWg.Done()
	}()

	// wait for pipeline to finish
	r.wg.Wait()

	// close err accumulator
	close(r.errChan)
	errWg.Wait()

	r.monitor.RecordRun(recipe, getDuration(), success)
	return multierr.Combine(errs...)
}

func (r *Runner) RunMultiple(recipes []Recipe) (faileds []string, err error) {
	faileds = []string{}
	for _, recipe := range recipes {
		if err := r.Run(recipe); err != nil {
			faileds = append(faileds, recipe.Name)
		}
	}
	return
}

func (r *Runner) runExtractor(ctx context.Context, task *Task, extractOut chan interface{}) {
	defer r.wg.Done()

	extractor, err := r.extractorFactory.Get(task.Name)
	if err != nil {
		r.errChan <- err
		close(extractOut)
		return
	}

	err = extractor.Extract(ctx, task.Config, extractOut)
	close(extractOut)

	if err != nil {
		err = r.newRunTaskError(*task, err)
		task.Status = TaskStatusFailed
	} else {
		task.Status = TaskStatusComplete
	}

	if err != nil {
		r.errChan <- err
	}
}

func (r *Runner) runProcessor(ctx context.Context, task *Task, in <-chan interface{}, out chan<- interface{}) {
	defer r.wg.Done()

	proc, err := r.processorFactory.Get(task.Name)
	if err != nil {
		r.errChan <- err
		close(out)
		return
	}

	err = proc.Process(ctx, task.Config, in, out)
	close(out)

	if err != nil {
		err = r.newRunTaskError(*task, err)
		task.Status = TaskStatusFailed
	} else {
		task.Status = TaskStatusComplete
	}

	if err != nil {
		r.errChan <- err
	}
}

func (r *Runner) runSink(ctx context.Context, task *Task, in <-chan interface{}){
	defer r.wg.Done()

	sink, err := r.sinkFactory.Get(task.Name)
	if err != nil {
		r.errChan <- err
		return
	}

	err = sink.Sink(ctx, task.Config, in)
	if err != nil {
		err = r.newRunTaskError(*task, err)
		task.Status = TaskStatusFailed
	} else {
		task.Status = TaskStatusComplete
	}

	if err != nil {
		r.errChan <- err
	}
}

func (r *Runner) newRunTaskError(task Task, err error) RunTaskError {
	return RunTaskError{
		Task: task,
		Err:  err,
	}
}

func (r *Runner) startDuration() func() int {
	start := time.Now()
	return func() int {
		return int(time.Now().Sub(start).Milliseconds())
	}
}
