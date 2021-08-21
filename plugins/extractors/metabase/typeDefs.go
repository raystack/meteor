package metabase

type Dashboard struct {
	ID          int `json:"id"`
	Urn         string
	Name        string  `json:"name"`
	Source      string  `default:"metabase"`
	Description string  `json:"description"`
	Charts      []Chart `json:"ordered_cards"`
}

type Chart struct {
	ID           int `json:"id"`
	Urn          string
	Source       string `default:"metabase"`
	DashboardUrn string
	DashboardID  int `json:"dashboard_id"`
}
