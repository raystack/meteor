package superset

type Dashboard struct {
	ChangedByName  string   `json:"changed_by_name"`
	ChangedByUrl   string   `json:"changed_by_url"`
	DashboardTitle string   `json:"dashboard_title"`
	ID             int      `json:"id"`
	JsonMetadata   string   `json:"json_metadata"`
	PositionJson   string   `json:"position_json"`
	Published      bool     `json:"published"`
	Slug           string   `json:"slug"`
	TableNames     string   `json:"table_names"`
	ThumbnailUrl   string   `json:"thumbnail_url"`
	URL            string   `json:"url"`
	Owners         []Owners `json:"owners"`
	Roles          []Roles  `json:"roles"`
}

type Chart struct {
	CacheTimeout        int      `json:"cache_timeout"`
	ChangedOn           string   `json:"changed_on"`
	Datasource          string   `json:"datasource"`
	Description         string   `json:"description"`
	DescriptionMarkdown string   `json:"description_markeddown"`
	FormData            struct{} `json:"form_data"`
	Modified            string   `json:"modified"`
	SliceId             int      `json:"slice_id"`
	SliceName           string   `json:"slice_name"`
	SliceUrl            string   `json:"slice_url"`
}

type Roles struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type Owners struct {
	FirstName string `json:"first_name"`
	ID        int    `json:"id"`
	LastName  string `json:"last_name"`
	Username  string `json:"username"`
}
