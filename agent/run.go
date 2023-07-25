package agent

import "github.com/goto/meteor/recipe"

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

// Run contains the json data
type Run struct {
	Recipe           recipe.Recipe `json:"recipe"`
	Error            error         `json:"error"`
	DurationInMs     int           `json:"duration_in_ms"`
	ExtractorRetries int           `json:"extractor_retries"`
	AssetsExtracted  int           `json:"assets_extracted"`
	RecordCount      int           `json:"record_count"`
	Success          bool          `json:"success"`
}
