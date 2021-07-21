package recipe

import (
	"errors"
	"time"

	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/core/processor"
	sinks "github.com/odpf/meteor/core/sink"
)

type Runner struct {
	extractor        *extractor.Extractor
	processorFactory *processor.Factory
	sinkFactory      *sinks.Factory
	monitor          Monitor
}

func NewRunner(
	extractor *extractor.Extractor,
	processorFactory *processor.Factory,
	sinkFactory *sinks.Factory,
	monitor Monitor) *Runner {
	if isNilMonitor(monitor) {
		monitor = new(defaultMonitor)
	}
	return &Runner{
		extractor:        extractor,
		processorFactory: processorFactory,
		sinkFactory:      sinkFactory,
		monitor:          monitor,
	}
}

func (r *Runner) Run(recipe Recipe) (run *Run, err error) {
	getDuration := r.startDuration()
	run = r.buildRun(recipe)
	success := true

	var data interface{}
	for i := 0; i < len(run.Tasks); i++ {
		data, err = r.runTask(&run.Tasks[i], data)

		if err != nil {
			success = false
			break
		}
	}

	duration := getDuration()
	r.monitor.RecordRun(recipe, duration, success)

	return
}

func (r *Runner) RunMultiple(recipes []Recipe) (faileds []string, err error) {
	faileds = []string{}
	for _, recipe := range recipes {
		_, err := r.Run(recipe)
		if err != nil {
			faileds = append(faileds, recipe.Name)
		}
	}
	return
}

func (r *Runner) runTask(task *Task, data interface{}) (result interface{}, err error) {
	result = data

	switch task.Type {
	case TaskTypeExtract:
		result, err = r.runExtractor(task.Name, task.Config)
	case TaskTypeProcess:
		result, err = r.runProcessor(task.Name, data, task.Config)
	case TaskTypeSink:
		err = r.runSink(task.Name, data, task.Config)
	default:
		err = errors.New("invalid task type")
	}

	if err != nil {
		err = r.newRunTaskError(*task, err)
		task.Status = TaskStatusFailed
	} else {
		task.Status = TaskStatusComplete
	}

	return result, err
}

func (r *Runner) runExtractor(name string, config map[string]interface{}) (result interface{}, err error) {
	return r.extractor.Extract(name, config)
}

func (r *Runner) runProcessor(name string, data interface{}, config map[string]interface{}) (result interface{}, err error) {
	proc, err := r.processorFactory.Get(name)
	if err != nil {
		return result, err
	}

	return proc.Process(data, config)
}

func (r *Runner) runSink(name string, data interface{}, config map[string]interface{}) (err error) {
	sink, err := r.sinkFactory.Get(name)
	if err != nil {
		return err
	}

	return sink.Sink(data, config)
}

func (r *Runner) buildRun(recipe Recipe) *Run {
	var tasks []Task

	tasks = append(tasks, Task{
		Type:   TaskTypeExtract,
		Status: TaskStatusReady,
		Name:   recipe.Source.Type,
		Config: recipe.Source.Config,
	})

	for _, processor := range recipe.Processors {
		tasks = append(tasks, Task{
			Type:   TaskTypeProcess,
			Status: TaskStatusReady,
			Name:   processor.Name,
			Config: processor.Config,
		})
	}

	for _, sink := range recipe.Sinks {
		tasks = append(tasks, Task{
			Type:   TaskTypeSink,
			Status: TaskStatusReady,
			Name:   sink.Name,
			Config: sink.Config,
		})
	}

	return &Run{
		Recipe: recipe,
		Tasks:  tasks,
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
