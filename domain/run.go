package domain

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
	Recipe Recipe
	Tasks  []Task
	Data   []map[string]interface{}
}
