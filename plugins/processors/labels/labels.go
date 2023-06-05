package labels

import (
	"context"
	_ "embed"

	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/salt/log"
)

//go:embed README.md
var summary string

type Config struct {
	Labels map[string]string `mapstructure:"labels" validate:"required"`
}

// Processor work in a list of data
type Processor struct {
	plugins.BasePlugin
	config Config
	logger log.Logger
}

var sampleConfig = `
# Append labels to asset
# labels:
#   fieldA: valueA
#   fieldB: valueB`

var info = plugins.Info{
	Description:  "Append labels to assets",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"processor", "transform", "labels"},
}

// New create a new processor
func New(logger log.Logger) *Processor {
	p := &Processor{
		logger: logger,
	}
	p.BasePlugin = plugins.NewBasePlugin(info, &p.config)

	return p
}

// Process processes the data
func (p *Processor) Init(ctx context.Context, config plugins.Config) error {
	return p.BasePlugin.Init(ctx, config)
}

// Process processes the data
func (p *Processor) Process(_ context.Context, src models.Record) (models.Record, error) {
	result, err := p.process(src)
	if err != nil {
		return src, err
	}

	return models.NewRecord(result), nil
}

func (p *Processor) process(record models.Record) (*v1beta2.Asset, error) {
	asset := record.Data()

	labels := asset.Labels
	if labels == nil {
		labels = make(map[string]string)
	}

	// update labels using value from config
	for key, value := range p.config.Labels {
		labels[key] = value
	}

	asset.Labels = labels

	return asset, nil
}

func init() {
	if err := registry.Processors.Register("labels", func() plugins.Processor {
		return New(plugins.GetLog())
	}); err != nil {
		return
	}
}
