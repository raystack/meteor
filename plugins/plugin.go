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

type Emit func(models.Record)

// Info represents the meta.yaml file of a plugin.
type Info struct {
	Description  string   `yaml:"description"`
	SampleConfig string   `yaml:"sample_config"`
	Tags         []string `yaml:"tags"`
	Summary      string   `yaml:"summary"`
}

type Plugin interface {
	// Info returns plugin's information.
	Info() Info

	// Validate checks if the given config is valid for the plugin.
	Validate(config map[string]interface{}) error

	// Init will be called once before running the plugin.
	// This is where you want to initiate any client or test any connection to external service.
	Init(ctx context.Context, config map[string]interface{}) error
}

// Extractor is a plugin that extracts data from a source.
type Extractor interface {
	Plugin
	Extract(ctx context.Context, emit Emit) (err error)
}

// Processor are the functions that are executed on the extracted data.
type Processor interface {
	Plugin
	Process(ctx context.Context, src models.Record) (dst models.Record, err error)
}

// Syncer is a plugin that can be used to sync data from one source to another.
type Syncer interface {
	Plugin
	Sink(ctx context.Context, batch []models.Record) (err error)
}

// ParseInfo parses the plugin's meta.yaml file and returns an plugin Info struct.
func ParseInfo(text string) (info Info, err error) {
	err = yaml.Unmarshal([]byte(text), &info)
	if err != nil {
		return
	}
	return
}
