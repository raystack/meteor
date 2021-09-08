package agent

import "github.com/odpf/meteor/recipe"

// TaskType is the type of a task
type TaskType string

const (
	// TaskTypeExtract is the type of a task that extracts a file
	TaskTypeExtract TaskType = "extract"
	// TaskTypeProcess is the type of a task that processes a file
	TaskTypeProcess TaskType = "process"
	// TaskTypeSink is the type of a task that sends a file to a sink
	TaskTypeSink    TaskType = "sink"
)

// Run contains the json data
type Run struct {
	Recipe recipe.Recipe `json:"recipe"`
	Error  error         `json:"error"`
}
