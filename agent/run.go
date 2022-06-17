package agent

import "github.com/odpf/meteor/recipe"

// TaskType is the type of task
type TaskType string

const (
	// TaskTypeExtract is the type of task that extracts a record
	TaskTypeExtract TaskType = "extract"
	// TaskTypeProcess is the type of task that processes a record
	TaskTypeProcess TaskType = "process"
	// TaskTypeSink is the type of task that sends a record to a sink
	TaskTypeSink TaskType = "sink"
)

type RunSink struct {
	Error   error               `json:"error"`
	Success bool                `json:"success"`
	Recipe  recipe.PluginRecipe `json:"recipe"`
}

// Run contains the json data
type Run struct {
	Recipe       recipe.Recipe `json:"recipe"`
	Error        error         `json:"error"`
	DurationInMs int           `json:"duration_in_ms"`
	RecordCount  int           `json:"record_count"`
	Success      bool          `json:"success"`
	Sinks        []RunSink     `json:"sink"`
}
