package recipe

type SourceRecipe struct {
	Type   string                 `yaml:"type" validate:"required"`
	Config map[string]interface{} `yaml:"config"`
}

type SinkRecipe struct {
	Name         string                 `yaml:"name" validate:"required"`
	BaseSinkName string                 `yaml:"base_sink_name"`
	Config       map[string]interface{} `yaml:"config"`
}

type ProcessorRecipe struct {
	Name   string                 `yaml:"name" validate:"required"`
	Config map[string]interface{} `yaml:"config"`
}

type Recipe struct {
	Name       string            `yaml:"name" validate:"required"`
	Source     SourceRecipe      `yaml:"source" validate:"required"`
	Sinks      []SinkRecipe      `yaml:"sinks" validate:"required,min=1"`
	Processors []ProcessorRecipe `yaml:"processors"`
}
