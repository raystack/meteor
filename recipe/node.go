package recipe

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

type RecipeNode struct {
	Name       yaml.Node    `json:"name" yaml:"name"`
	Source     PluginNode   `json:"source" yaml:"source"`
	Sinks      []PluginNode `json:"sinks" yaml:"sinks"`
	Processors []PluginNode `json:"processors" yaml:"processors"`
}

type PluginNode struct {
	Name   yaml.Node            `json:"name" yaml:"name"`
	Config map[string]yaml.Node `json:"config" yaml:"config"`
}

func (plug *PluginNode) toDecode() map[string]interface{} {
	config := make(map[string]interface{})

	for i, j := range plug.Config {
		config[i] = j.Value
	}
	return config
}

func (node RecipeNode) toRecipe() (recipe Recipe, err error) {
	var sourceConfig map[string]interface{}
	sourceConfig = node.Source.toDecode()
	//if err = node.Source.Config.Decode(&sourceConfig); err != nil {
	//	err = fmt.Errorf("error decoding yaml node to source :%w", err)
	//	return
	//}
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

func (node RecipeNode) toProcessors() (processors []PluginRecipe, err error) {
	for _, processor := range node.Processors {
		var config map[string]interface{}
		config = processor.toDecode()
		//err = processor.Config.Decode(&config)
		//if err != nil {
		//	err = fmt.Errorf("error decoding yaml node to processor :%w", err)
		//	return
		//}
		processors = append(processors, PluginRecipe{
			Name:   processor.Name.Value,
			Config: config,
			Node:   processor,
		})
	}
	return
}

func (node RecipeNode) toSinks() (sinks []PluginRecipe, err error) {
	for _, sink := range node.Sinks {
		var config map[string]interface{}
		config = sink.toDecode()
		//err = sink.Config.Decode(&config)
		//if err != nil {
		//	err = fmt.Errorf("error decoding yaml node to sink :%w", err)
		//	return
		//}
		sinks = append(sinks, PluginRecipe{
			Name:   sink.Name.Value,
			Config: config,
			Node:   sink,
		})
	}
	return
}
