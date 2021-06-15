package domain

type SourceRecipe struct {
	Type   string                 `json:"type" validate:"required"`
	Config map[string]interface{} `json:"config"`
}

type SinkRecipe struct {
	Name         string                 `json:"name" validate:"required"`
	BaseSinkName string                 `json:"base_sink_name"`
	Config       map[string]interface{} `json:"config"`
}

type ProcessorRecipe struct {
	Name   string                 `json:"name" validate:"required"`
	Config map[string]interface{} `json:"config"`
}

type Recipe struct {
	Name       string            `json:"name" validate:"required"`
	Source     SourceRecipe      `json:"source" validate:"required"`
	Sinks      []SinkRecipe      `json:"sinks" validate:"required,min=1"`
	Processors []ProcessorRecipe `json:"processors"`
}

type RecipeStore interface {
	GetByName(string) (Recipe, error)
	Create(Recipe) error
}
