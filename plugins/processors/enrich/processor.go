package enrich

import (
	"context"
	_ "embed"

	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/meteor/utils"
	"github.com/goto/salt/log"
)

//go:embed README.md
var summary string

type Config struct {
	Attributes map[string]interface{} `mapstructure:"attributes" validate:"required"`
}

// Processor work in a list of data
type Processor struct {
	plugins.BasePlugin
	config Config
	logger log.Logger
}

var sampleConfig = `
# Enrichment configuration
# attributes:
#   fieldA: valueA
#   fieldB: valueB`

var info = plugins.Info{
	Description:  "Append custom fields to records",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"processor", "transform"},
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
func (p *Processor) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = p.BasePlugin.Init(ctx, config); err != nil {
		return err
	}

	return
}

// Process processes the data
func (p *Processor) Process(ctx context.Context, src models.Record) (dst models.Record, err error) {
	result, err := p.process(src)
	if err != nil {
		return src, err
	}

	return models.NewRecord(result), nil
}

func (p *Processor) process(record models.Record) (*v1beta2.Asset, error) {
	data := record.Data()
	p.logger.Debug("enriching record", "record", data.Urn)
	customProps := utils.GetAttributes(data)

	// update custom properties using value from config
	for key, value := range p.config.Attributes {
		stringVal, ok := value.(string)
		if ok {
			customProps[key] = stringVal
		}
	}

	// save custom properties
	result, err := utils.SetAttributes(data, customProps)
	if err != nil {
		return data, err
	}

	return result, nil
}

func init() {
	if err := registry.Processors.Register("enrich", func() plugins.Processor {
		return New(plugins.GetLog())
	}); err != nil {
		return
	}
}
