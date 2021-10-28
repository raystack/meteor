package metabase

import "time"

const (
	timestampFormat = "2006-01-02T15:04:05.999999Z"
)

type Dashboard struct {
	BaseModel
	CreatorID    int    `json:"creator_id"`
	CollectionID int    `json:"collection_id"`
	Name         string `json:"name"`
	Description  string `json:"description"`
	OrderedCards []struct {
		Card Card `json:"card"`
	} `json:"ordered_cards"`
	LastEditInfo struct {
		Id        string    `json:"id"`
		Email     string    `json:"email"`
		Timestamp time.Time `json:"timestamp"`
	}
}

type Card struct {
	BaseModel
	CollectionID         int    `json:"collection_id"`
	DatabaseID           int    `json:"database_id"`
	TableID              int    `json:"table_id"`
	CreatorID            int    `json:"creator_id"`
	Name                 string `json:"name"`
	QueryAverageDuration int    `json:"query_average_duration"`
	Description          string `json:"description"`
	Display              string `json:"display"`
	DatasetQuery         struct {
		Type  string      `json:"type"`
		Query interface{} `json:"query"`
	} `json:"dataset_query"`
	Archived bool `json:"archived"`
}

type CardResultMetadata struct {
	BaseModel
	Name          string   `json:"name"`
	DisplayName   string   `json:"display_name"`
	BaseType      string   `json:"base_type"`
	EffectiveType string   `json:"effective_type"`
	SemanticType  string   `json:"semantic_type"`
	Description   string   `json:"description"`
	Unit          string   `json:"unit"`
	FieldRef      []string `json:"field_ref"`
}

type Table struct {
	BaseModel
	DbID        int      `json:"db_id"`
	Name        string   `json:"name"`
	DisplayName string   `json:"display_name"`
	Description string   `json:"description"`
	FieldOrder  string   `json:"field_order"`
	EntityType  string   `json:"entity_type"`
	Schema      string   `json:"schema"`
	Active      bool     `json:"active"`
	Db          Database `json:"db"`
}

type Database struct {
	BaseModel
	DbID                     int      `json:"db_id"`
	Name                     string   `json:"name"`
	Features                 []string `json:"features"`
	Description              string   `json:"description"`
	Timezone                 string   `json:"timezone"`
	Engine                   string   `json:"engine"`
	MetadataSyncSchedule     string   `json:"metadata_sync_schedule"`
	CacheFieldValuesSchedule string   `json:"cache_field_values_schedule"`
	AutoRunQueries           bool     `json:"auto_run_queries"`
	IsFullSync               bool     `json:"is_full_sync"`
	IsSample                 bool     `json:"is_sample"`
	IsOnDemand               bool     `json:"is_on_demand"`
	Details                  struct {
		Db string `json:"db"`
	} `json:"details"`
}

type BaseModel struct {
	ID              int    `json:"id"`
	CreatedAtString string `json:"created_at"`
	UpdatedAtString string `json:"updated_at"`
}

func (m *BaseModel) CreatedAt() (time.Time, error) {
	return time.Parse(timestampFormat, m.CreatedAtString)
}

func (m *BaseModel) UpdatedAt() (time.Time, error) {
	return time.Parse(timestampFormat, m.UpdatedAtString)
}
