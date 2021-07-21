package recipe

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
	Type   string                 `json:"type"`
	Name   string                 `json:"name"`
	Config map[string]interface{} `json:"config"`
	Status string                 `json:"status"`
}

type Run struct {
	Recipe Recipe        `json:"recipe"`
	Tasks  []Task        `json:"tasks"`
	Data   []interface{} `json:"-"`
}
