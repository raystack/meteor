package domain

type SourceRecipe struct {
	Type   string `validate:"required"`
	Config map[string]interface{}
}

type SinkRecipe struct {
	Name         string `validate:"required"`
	BaseSinkName string
	Config       map[string]interface{}
}

type ProcessorRecipe struct {
	Name   string `validate:"required"`
	Config map[string]interface{}
}

type Recipe struct {
	Name       string       `validate:"required"`
	Source     SourceRecipe `validate:"required"`
	Sinks      []SinkRecipe `validate:"required,min=1"`
	Processors []ProcessorRecipe
}
