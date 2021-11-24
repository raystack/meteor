package optimus

// TODO: Remove all of these models and use Optimus' models when they are already available.

type GetJobTaskRequest struct {
	ProjectName string
	Namespace   string
	JobName     string
}

type GetJobTaskResponse struct {
	Task *JobTask
}

type JobTask struct {
	Name         string               `json:"name"`
	Description  string               `json:"description"`
	Image        string               `json:"image"`
	Destination  *JobTaskDestination  `json:"destination"`
	Dependencies []*JobTaskDependency `json:"dependencies"`
}

type JobTaskDestination struct {
	Destination string `json:"destination"`
	Type        string `json:"type"`
}

type JobTaskDependency struct {
	Dependency string `json:"dependency"`
}
