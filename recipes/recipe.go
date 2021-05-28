package recipes

import "fmt"

type Source struct {
	Type   string
	Config map[string]interface{}
}

type Sink struct {
	Name         string
	BaseSinkName string
	Config       map[string]interface{}
}

type Processor struct {
	Name   string
	Config map[string]interface{}
}

type Recipe struct {
	Name       string
	Source     Source
	Sinks      []Sink
	Processors []Processor
}

type NotFoundError struct {
	RecipeName string
}

func (err NotFoundError) Error() string {
	return fmt.Sprintf("could not find recipe with name \"%s\"", err.RecipeName)
}
