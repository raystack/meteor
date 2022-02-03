package recipe

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

type RecipeNode struct {
	Name       yaml.Node             `json:"name" yaml:"name"`
	Source     SourceRecipeNode      `json:"source" yaml:"source"`
	Sinks      []SinkRecipeNode      `json:"sinks" yaml:"sinks"`
	Processors []ProcessorRecipeNode `json:"processors" yaml:"processors"`
}
type SourceRecipeNode struct {
	Type   yaml.Node `json:"type" yaml:"type"`
	Config yaml.Node `json:"config" yaml:"config"`
}
type ProcessorRecipeNode struct {
	Name   yaml.Node `json:"name" yaml:"name"`
	Config yaml.Node `json:"config" yaml:"config"`
}
type SinkRecipeNode struct {
	Name   yaml.Node `json:"name" yaml:"name"`
	Config yaml.Node `json:"config" yaml:"config"`
}

func (node RecipeNode) toRecipe() (recipe Recipe, err error) {
	var sourceConfig map[string]interface{}
	err = node.Source.Config.Decode(&sourceConfig)
	if err != nil {
		err = fmt.Errorf("error decoding yaml node to source :%w", err)
		return
	}
	processors, err := node.toProcessors()
	if err != nil {
		err = fmt.Errorf("error building processors :%w", err)
		return
	}
	sinks, err := node.toSinks()
	if err != nil {
		err = fmt.Errorf("error building sinks :%w", err)
		return
	}

	recipe = Recipe{
		Name: node.Name.Value,
		Source: SourceRecipe{
			Type:   node.Source.Type.Value,
			Config: sourceConfig,
			Node:   node.Source,
		},
		Sinks:      sinks,
		Processors: processors,
		Node:       node,
	}

	return
}

func (node RecipeNode) toProcessors() (processors []ProcessorRecipe, err error) {
	for _, ns := range node.Processors {
		var config map[string]interface{}
		err = ns.Config.Decode(&config)
		if err != nil {
			err = fmt.Errorf("error decoding yaml node to processor :%w", err)
			return
		}
		processors = append(processors, ProcessorRecipe{
			Name:   ns.Name.Value,
			Config: config,
			Node:   ns,
		})
	}
	return
}

func (node RecipeNode) toSinks() (sinks []SinkRecipe, err error) {
	for _, ns := range node.Sinks {
		var config map[string]interface{}
		err = ns.Config.Decode(&config)
		if err != nil {
			err = fmt.Errorf("error decoding yaml node to sink :%w", err)
			return
		}
		sinks = append(sinks, SinkRecipe{
			Name:   ns.Name.Value,
			Config: config,
			Node:   ns,
		})
	}
	return
}
