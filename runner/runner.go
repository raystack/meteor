package runner

import (
	"errors"

	"github.com/odpf/meteor/extractors"
	"github.com/odpf/meteor/processors"
	"github.com/odpf/meteor/recipes"
	"github.com/odpf/meteor/sinks"
)

type Runner struct {
	extractorStore *extractors.Store
	processorStore *processors.Store
	sinkStore      *sinks.Store
}

func New(extractorStore *extractors.Store, processorStore *processors.Store, sinkStore *sinks.Store) *Runner {
	return &Runner{
		extractorStore: extractorStore,
		processorStore: processorStore,
		sinkStore:      sinkStore,
	}
}

func (r *Runner) Run(recipe recipes.Recipe) (*Run, error) {
	run := r.buildRun(recipe)

	for i := 0; i < len(run.Tasks); i++ {
		data, err := r.runTask(&run.Tasks[i], run.Data)
		run.Data = data

		if err != nil {
			return run, err
		}
	}

	return run, nil
}

func (r *Runner) runTask(task *Task, data []map[string]interface{}) (result []map[string]interface{}, err error) {
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
		err = newRunTaskError(*task, err)
		task.Status = TaskStatusFailed
	} else {
		task.Status = TaskStatusComplete
	}

	return result, err
}

func (r *Runner) runExtractor(name string, config map[string]interface{}) (result []map[string]interface{}, err error) {
	extractor, err := r.extractorStore.Find(name)
	if err != nil {
		return result, err
	}

	return extractor.Extract(config)
}

func (r *Runner) runProcessor(name string, data []map[string]interface{}, config map[string]interface{}) (result []map[string]interface{}, err error) {
	processor, err := r.processorStore.Find(name)
	if err != nil {
		return result, err
	}

	return processor.Process(data, config)
}

func (r *Runner) runSink(name string, data []map[string]interface{}, config map[string]interface{}) (err error) {
	sink, err := r.sinkStore.Find(name)
	if err != nil {
		return err
	}

	return sink.Sink(data, config)
}

func (r *Runner) buildRun(recipe recipes.Recipe) *Run {
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
