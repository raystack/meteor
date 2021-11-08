package optimus

type GetProjectsResponse struct {
	Projects []Project `json:"projects"`
}

type GetNamespacesResponse struct {
	Namespaces []Namespace `json:"namespaces"`
}

type GetJobsResponse struct {
	Jobs []Job `json:"jobs"`
}

type GetJobTaskResponse struct {
	Task JobTask `json:"task"`
}

type Project struct {
	Name    string                 `json:"name"`
	Config  map[string]interface{} `json:"config"`
	Secrets []interface{}          `json:"secrets"`
}

type Namespace struct {
	Name   string                 `json:"name"`
	Config map[string]interface{} `json:"config"`
}

type Job struct {
	Version          int                      `json:"version"`
	Name             string                   `json:"name"`
	Config           []map[string]interface{} `json:"config"`
	Owner            string                   `json:"owner"`
	StartDate        string                   `json:"startDate"`
	EndDate          string                   `json:"endDate"`
	Interval         string                   `json:"interval"`
	DependsOnPast    bool                     `json:"dependsOnPast"`
	CatchUp          bool                     `json:"catchUp"`
	TaskName         string                   `json:"taskName"`
	WindowSize       string                   `json:"windowSize"`
	WindowOffset     string                   `json:"windowOffset"`
	WindowTruncateTo string                   `json:"windowTruncateTo"`
	Dependencies     []interface{}            `json:"dependencies"`
	Assets           struct {
		QuerySQL string `json:"query.sql"`
	} `json:"assets"`
	Hooks       []interface{}          `json:"hooks"`
	Description string                 `json:"description"`
	Labels      map[string]string      `json:"labels"`
	Behavior    map[string]interface{} `json:"behavior"`
}

type JobTask struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Image       string `json:"image"`
	Destination struct {
		Destination string `json:"destination"`
		Type        string `json:"type"`
	} `json:"destination"`
	Dependencies []struct {
		Dependency string `json:"dependency"`
	} `json:"dependencies"`
}
