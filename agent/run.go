package agent

import "github.com/raystack/meteor/recipe"

// Run contains the json data
type Run struct {
	Recipe           recipe.Recipe  `json:"recipe"`
	Error            error          `json:"error"`
	DurationInMs     int            `json:"duration_in_ms"`
	ExtractorRetries int            `json:"extractor_retries"`
	RecordsExtracted  int            `json:"records_extracted"`
	RecordCount      int            `json:"record_count"`
	Success          bool           `json:"success"`
	EntityTypes      map[string]int `json:"entity_types,omitempty"`
	DryRun           bool           `json:"dry_run,omitempty"`
}
