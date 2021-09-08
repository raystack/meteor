package recipe

// SourceRecipe contains the json data for a recipe that is used to generate
// the source code for a recipe.
type SourceRecipe struct {
	Type   string                 `json:"type" yaml:"type" validate:"required"`
	Config map[string]interface{} `json:"config" yaml:"config"`
}

// SinkRecipe contains the json data for a recipe that is being used for
// generating the sink code for a recipe.
type SinkRecipe struct {
	Name   string                 `json:"name" yaml:"name" validate:"required"`
	Config map[string]interface{} `json:"config" yaml:"config"`
}

// ProcessorRecipe contains the json data for a recipe that is being used for
// generating the processor code for a recipe.
type ProcessorRecipe struct {
	Name   string                 `json:"name" yaml:"name" validate:"required"`
	Config map[string]interface{} `json:"config" yaml:"config"`
}

// Recipe contains the json data for a recipe 
type Recipe struct {
	Name       string            `json:"name" yaml:"name" validate:"required"`
	Source     SourceRecipe      `json:"source" yaml:"source" validate:"required"`
	Sinks      []SinkRecipe      `json:"sinks" yaml:"sinks" validate:"required,min=1"`
	Processors []ProcessorRecipe `json:"processors" yaml:"processors"`
}
