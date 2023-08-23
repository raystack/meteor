package models

import (
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

type Dashboard struct {
	ID           int          `json:"id"`
	CreatorID    int          `json:"creator_id"`
	CollectionID int          `json:"collection_id"`
	Name         string       `json:"name"`
	Description  string       `json:"description"`
	CreatedAt    MetabaseTime `json:"created_at"`
	UpdatedAt    MetabaseTime `json:"updated_at"`
	OrderedCards []struct {
		CardID int  `json:"card_id"`
		Card   Card `json:"card"`
	} `json:"ordered_cards"`
	LastEditInfo struct {
		Id        string    `json:"id"`
		Email     string    `json:"email"`
		Timestamp time.Time `json:"timestamp"`
	}
}

type Card struct {
	ID                   int              `json:"id"`
	CollectionID         int              `json:"collection_id"`
	DatabaseID           int              `json:"database_id"`
	TableID              int              `json:"table_id"`
	CreatorID            int              `json:"creator_id"`
	Name                 string           `json:"name"`
	QueryAverageDuration int              `json:"query_average_duration"`
	Description          string           `json:"description"`
	Display              string           `json:"display"`
	CreatedAt            MetabaseTime     `json:"created_at"`
	UpdatedAt            MetabaseTime     `json:"updated_at"`
	DatasetQuery         CardDatasetQuery `json:"dataset_query"`
	Archived             bool             `json:"archived"`
}

type CardResultMetadata struct {
	ID            int          `json:"id"`
	Name          string       `json:"name"`
	DisplayName   string       `json:"display_name"`
	BaseType      string       `json:"base_type"`
	EffectiveType string       `json:"effective_type"`
	SemanticType  string       `json:"semantic_type"`
	Description   string       `json:"description"`
	Unit          string       `json:"unit"`
	FieldRef      []string     `json:"field_ref"`
	CreatedAt     MetabaseTime `json:"created_at"`
	UpdatedAt     MetabaseTime `json:"updated_at"`
}

type Table struct {
	ID          int          `json:"id"`
	DbID        int          `json:"db_id"`
	Name        string       `json:"name"`
	DisplayName string       `json:"display_name"`
	Description string       `json:"description"`
	FieldOrder  string       `json:"field_order"`
	EntityType  string       `json:"entity_type"`
	Schema      string       `json:"schema"`
	Active      bool         `json:"active"`
	CreatedAt   MetabaseTime `json:"created_at"`
	UpdatedAt   MetabaseTime `json:"updated_at"`
	Db          Database     `json:"db"`
}

type Database struct {
	ID                       int          `json:"id"`
	DbID                     int          `json:"db_id"`
	Name                     string       `json:"name"`
	Features                 []string     `json:"features"`
	Description              string       `json:"description"`
	Timezone                 string       `json:"timezone"`
	Engine                   string       `json:"engine"`
	MetadataSyncSchedule     string       `json:"metadata_sync_schedule"`
	CacheFieldValuesSchedule string       `json:"cache_field_values_schedule"`
	AutoRunQueries           bool         `json:"auto_run_queries"`
	IsFullSync               bool         `json:"is_full_sync"`
	IsSample                 bool         `json:"is_sample"`
	IsOnDemand               bool         `json:"is_on_demand"`
	CreatedAt                MetabaseTime `json:"created_at"`
	UpdatedAt                MetabaseTime `json:"updated_at"`
	Details                  struct {
		Db                string      `json:"db"`
		Host              string      `json:"host"`
		Port              int         `json:"port"`
		Dbname            string      `json:"dbname"`
		User              string      `json:"user"`
		Password          string      `json:"password"`
		SSL               bool        `json:"ssl"`
		AdditionalOptions interface{} `json:"additional-options"`
		TunnelEnabled     bool        `json:"tunnel-enabled"`
		ProjectID         string      `json:"project-id"`
		DatasetID         string      `json:"dataset-id"`
	} `json:"details"`
}

type Collection struct {
	ID          int          `json:"id"`
	Name        string       `json:"name"`
	Color       string       `json:"color"`
	Description string       `json:"description"`
	CreatedAt   MetabaseTime `json:"created_at"`
	UpdatedAt   MetabaseTime `json:"updated_at"`
}

type CardDatasetQuery struct {
	Type  string `json:"type"`
	Query struct {
		SourceTable int           `json:"source-table"`
		Filter      []interface{} `json:"filter"`
	} `json:"query"`
	Native   NativeDatasetQuery `json:"native"`
	Database int                `json:"database"`
}

type NativeDatasetQuery struct {
	Query        string                      `json:"query"`
	TemplateTags map[string]QueryTemplateTag `json:"template-tags"`
}

type QueryTemplateTag struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	Name        string `json:"name"`
	DisplayName string `json:"display-name"`
}

type MetabaseTime time.Time

func (mt *MetabaseTime) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), "\"")
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return err
	}
	*mt = MetabaseTime(t)
	return nil
}

func (mt MetabaseTime) ToPB() *timestamppb.Timestamp {
	return timestamppb.New((time.Time)(mt))
}
