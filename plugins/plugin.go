package plugins

import (
	"context"

	"gopkg.in/yaml.v3"
)

type PluginType string

const (
	PluginTypeExtractor PluginType = "extractor"
	PluginTypeProcessor PluginType = "processor"
	PluginTypeSink      PluginType = "sink"
)

// Info represents the meta.yaml file of a plugin.
type Info struct {
	Title        string `yaml:"title"`
	Version      string `yaml:"version"`
	Description  string `yaml:"description"`
	SampleConfig string `yaml:"sample_config"`
}

// Extractor is a plugin that extracts data from a source.
type Extractor interface {
	Extract(ctx context.Context, config map[string]interface{}, out chan<- interface{}) (err error)
}

// Processors are the functions that are executed on the extracted data.
type Processor interface {
	Process(ctx context.Context, config map[string]interface{}, in <-chan interface{}, out chan<- interface{}) (err error)
}

// Syncer is a plugin that can be used to sync data from one source to another.
type Syncer interface {
	Info() (Info, error)
	Validate(config map[string]interface{}) error
	Sink(ctx context.Context, config map[string]interface{}, in <-chan interface{}) (err error)
}

// ParseInfo parses the plugin's meta.yaml file and returns an plugin Info struct.
func ParseInfo(text string) (info Info, err error) {
	err = yaml.Unmarshal([]byte(text), &info)
	if err != nil {
		return
	}
	return
}
