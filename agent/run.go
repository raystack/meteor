package agent

import "github.com/odpf/meteor/recipe"

type TaskType string

const (
	TaskTypeExtract TaskType = "extract"
	TaskTypeProcess TaskType = "process"
	TaskTypeSink    TaskType = "sink"
)

type Run struct {
	Recipe recipe.Recipe `json:"recipe"`
	Error  error         `json:"error"`
}
