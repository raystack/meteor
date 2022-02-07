package recipe

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// RecipeNode contains the json data for a recipe node
type RecipeNode struct {
	Name       yaml.Node    `json:"name" yaml:"name"`
	Source     PluginNode   `json:"source" yaml:"source"`
	Sinks      []PluginNode `json:"sinks" yaml:"sinks"`
	Processors []PluginNode `json:"processors" yaml:"processors"`
}

// PluginNode contains the json data for a recipe node that is being used for
// generating the plugins code for a recipe.
type PluginNode struct {
	Name   yaml.Node            `json:"name" yaml:"name"`
	Config map[string]yaml.Node `json:"config" yaml:"config"`
}

// decodeConfig decodes the plugins config
func (plug *PluginNode) decodeConfig() map[string]interface{} {
	config := make(map[string]interface{})

	for key, val := range plug.Config {
		config[key] = val.Value
	}
	return config
}

// toRecipe passes the value from RecipeNode to Recipe
func (node RecipeNode) toRecipe() (recipe Recipe, err error) {
	sourceConfig := node.Source.decodeConfig()
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
		Source: PluginRecipe{
			Name:   node.Source.Name.Value,
			Config: sourceConfig,
			Node:   node.Source,
		},
		Sinks:      sinks,
		Processors: processors,
		Node:       node,
	}

	return
}

// toProcessors passes the value of processor PluginNode to its PluginRecipe
func (node RecipeNode) toProcessors() (processors []PluginRecipe, err error) {
	for _, processor := range node.Processors {
		processorConfig := processor.decodeConfig()
		processors = append(processors, PluginRecipe{
			Name:   processor.Name.Value,
			Config: processorConfig,
			Node:   processor,
		})
	}
	return
}

// toSinks passes the value of sink PluginNode to its PluginRecipe
func (node RecipeNode) toSinks() (sinks []PluginRecipe, err error) {
	for _, sink := range node.Sinks {
		sinkConfig := sink.decodeConfig()
		sinks = append(sinks, PluginRecipe{
			Name:   sink.Name.Value,
			Config: sinkConfig,
			Node:   sink,
		})
	}
	return
}
