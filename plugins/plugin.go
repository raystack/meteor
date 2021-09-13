package plugins

import (
	"context"

	"github.com/odpf/meteor/models"
	"gopkg.in/yaml.v3"
)

// PluginType is the type of plugin.
type PluginType string

// PluginType names
const (
	PluginTypeExtractor PluginType = "extractor"
	PluginTypeProcessor PluginType = "processor"
	PluginTypeSink      PluginType = "sink"
)

// Info represents the meta.yaml file of a plugin.
type Info struct {
	Description  string   `yaml:"description"`
	SampleConfig string   `yaml:"sample_config"`
	Tags         []string `yaml:"tags"`
	Summary      string   `yaml:"summary"`
}

// Extractor is a plugin that extracts data from a source.
type Extractor interface {
	Info() Info
	Validate(config map[string]interface{}) error
	Extract(ctx context.Context, config map[string]interface{}, out chan<- models.Record) (err error)
}

// Processor are the functions that are executed on the extracted data.
type Processor interface {
	Info() Info
	Validate(config map[string]interface{}) error
	Process(ctx context.Context, config map[string]interface{}, in <-chan models.Record, out chan<- models.Record) (err error)
}

// Syncer is a plugin that can be used to sync data from one source to another.
type Syncer interface {
	Info() Info
	Validate(config map[string]interface{}) error
	Sink(ctx context.Context, config map[string]interface{}, in <-chan models.Record) (err error)
}

// ParseInfo parses the plugin's meta.yaml file and returns an plugin Info struct.
func ParseInfo(text string) (info Info, err error) {
	err = yaml.Unmarshal([]byte(text), &info)
	if err != nil {
		return
	}
	return
}
