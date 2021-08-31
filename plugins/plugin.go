package plugins

import "context"

type PluginType string

const (
	PluginTypeExtractor PluginType = "extractor"
	PluginTypeProcessor PluginType = "processor"
	PluginTypeSink      PluginType = "sink"
)

type PluginInfo struct {
	Name         string `yaml:"name"`
	Version      string `yaml:"version"`
	Description  string `yaml:"description"`
	SampleConfig string `yaml:"sample_config"`
}

type Extractor interface {
	Extract(ctx context.Context, config map[string]interface{}, out chan<- interface{}) (err error)
}

type Processor interface {
	Process(ctx context.Context, config map[string]interface{}, in <-chan interface{}, out chan<- interface{}) (err error)
}

type Syncer interface {
	Sink(ctx context.Context, config map[string]interface{}, in <-chan interface{}) (err error)
}
