package enrich

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/raystack/meteor/models"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
	"google.golang.org/protobuf/types/known/structpb"
)

//go:embed README.md
var summary string

type Config struct {
	Attributes map[string]any `mapstructure:"attributes" validate:"required"`
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
	Tags:         []string{"oss", "transform"},
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
	entity := src.Entity()
	p.logger.Debug("enriching record", "record", entity.GetUrn())

	props := entity.GetProperties()
	var customProps map[string]any
	if props != nil {
		customProps = props.AsMap()
	}
	if customProps == nil {
		customProps = make(map[string]any)
	}

	// update custom properties using value from config
	for key, value := range p.config.Attributes {
		stringVal, ok := value.(string)
		if ok {
			customProps[key] = stringVal
		}
	}

	// save custom properties
	newProps, err := structpb.NewStruct(customProps)
	if err != nil {
		return src, fmt.Errorf("set properties: %w", err)
	}
	entity.Properties = newProps

	return models.NewRecord(entity, src.Edges()...), nil
}

func init() {
	if err := registry.Processors.Register("enrich", func() plugins.Processor {
		return New(plugins.GetLog())
	}); err != nil {
		return
	}
}
