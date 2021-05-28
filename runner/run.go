package runner

import (
	"fmt"

	"github.com/odpf/meteor/recipes"
)

var (
	TaskTypeExtract = "extract"
	TaskTypeProcess = "process"
	TaskTypeSink    = "sink"
)

var (
	TaskStatusReady    = "ready"
	TaskStatusComplete = "complete"
	TaskStatusFailed   = "failed"
)

type Task struct {
	Type   string
	Name   string
	Config map[string]interface{}
	Status string
}

type Run struct {
	Recipe recipes.Recipe
	Tasks  []Task
	Data   []map[string]interface{}
}

type RunTaskError struct {
	task Task
	err  error
}

func newRunTaskError(task Task, err error) RunTaskError {
	return RunTaskError{
		task: task,
		err:  err,
	}
}

func (e RunTaskError) Error() string {
	return fmt.Sprintf(
		"error running %s task \"%s\": %s",
		e.task.Type,
		e.task.Name,
		e.err)
}
