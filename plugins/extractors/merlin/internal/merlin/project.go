package merlin

type Project struct {
	ID                int64    `json:"id"`
	Name              string   `json:"name"`
	MlflowTrackingURL string   `json:"mlflow_tracking_url"`
	Administrators    []string `json:"administrators"` // List of emails
	Readers           []string `json:"readers"`
	Team              string   `json:"team"`
	Stream            string   `json:"stream"`
	Labels            []Label  `json:"labels"`
	CreatedAt         string   `json:"created_at"`
	UpdatedAt         string   `json:"updated_at"`
}

type Label struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
