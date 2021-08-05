package recipe

type TaskType string

const (
	TaskTypeExtract TaskType = "extract"
	TaskTypeProcess TaskType = "process"
	TaskTypeSink    TaskType = "sink"
)

type Run struct {
	Recipe Recipe `json:"recipe"`
	Error  error  `json:"error"`
}
