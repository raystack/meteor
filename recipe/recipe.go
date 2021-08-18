package recipe

type SourceRecipe struct {
	Type   string                 `json:"type" yaml:"type" validate:"required"`
	Config map[string]interface{} `json:"config" yaml:"config"`
}

type SinkRecipe struct {
	Name   string                 `json:"name" yaml:"name" validate:"required"`
	Config map[string]interface{} `json:"config" yaml:"config"`
}

type ProcessorRecipe struct {
	Name   string                 `json:"name" yaml:"name" validate:"required"`
	Config map[string]interface{} `json:"config" yaml:"config"`
}

type Recipe struct {
	Name       string            `json:"name" yaml:"name" validate:"required"`
	Source     SourceRecipe      `json:"source" yaml:"source" validate:"required"`
	Sinks      []SinkRecipe      `json:"sinks" yaml:"sinks" validate:"required,min=1"`
	Processors []ProcessorRecipe `json:"processors" yaml:"processors"`
}
