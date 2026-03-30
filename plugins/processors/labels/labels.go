package labels

import (
	"context"
	_ "embed"

	"github.com/raystack/meteor/models"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
	"google.golang.org/protobuf/types/known/structpb"
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
func (p *Processor) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = p.BasePlugin.Init(ctx, config); err != nil {
		return err
	}

	return
}

// Process processes the data
func (p *Processor) Process(ctx context.Context, src models.Record) (dst models.Record, err error) {
	entity := src.Entity()

	// Get existing labels from properties, or create new map
	props := entity.GetProperties()
	if props == nil {
		props = &structpb.Struct{Fields: make(map[string]*structpb.Value)}
		entity.Properties = props
	}

	// Get existing labels map from properties
	labels := make(map[string]interface{})
	if labelsVal, ok := props.GetFields()["labels"]; ok {
		if labelsStruct := labelsVal.GetStructValue(); labelsStruct != nil {
			for k, v := range labelsStruct.GetFields() {
				labels[k] = v.GetStringValue()
			}
		}
	}

	// update labels using value from config
	for key, value := range p.config.Labels {
		labels[key] = value
	}

	// Set labels back into properties
	labelsStruct, err := structpb.NewStruct(labels)
	if err != nil {
		return src, err
	}
	props.Fields["labels"] = structpb.NewStructValue(labelsStruct)

	return models.NewRecord(entity, src.Edges()...), nil
}

func init() {
	if err := registry.Processors.Register("labels", func() plugins.Processor {
		return New(plugins.GetLog())
	}); err != nil {
		return
	}
}
