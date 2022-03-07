package redash

type Dashboard struct {
	ID         int    `json:"id"`
	UserID     int    `json:"user_id"`
	Name       string `json:"name"`
	Version    string `json:"version"`
	CreatedAt  string `json:"created_at"`
	UpdatedAt  string `json:"updated_at"`
	Filter     bool   `json:"dashboard_filters_enabled"`
	Slug       string `json:"slug"`
	IsArchived bool   `json:"is_archived"`
	IsDraft    bool   `json:"is_draft"`
	Layout     []int  `json:"layout"`
	Widgets    []int  `json:"widgets"`
}

// https://github1s.com/getredash/redash/blob/HEAD/redash/handlers/dashboards.py
