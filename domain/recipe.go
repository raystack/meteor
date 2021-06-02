package domain

type SourceRecipe struct {
	Type   string
	Config map[string]interface{}
}

type SinkRecipe struct {
	Name         string
	BaseSinkName string
	Config       map[string]interface{}
}

type ProcessorRecipe struct {
	Name   string
	Config map[string]interface{}
}

type Recipe struct {
	Name       string
	Source     SourceRecipe
	Sinks      []SinkRecipe
	Processors []ProcessorRecipe
}
