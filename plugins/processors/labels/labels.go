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
# Append labels to entity
# labels:
#   fieldA: valueA
#   fieldB: valueB`

var info = plugins.Info{
	Description:  "Append labels to entities.",
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

	// Get current properties as map
	var propMap map[string]any
	if entity.GetProperties() != nil {
		propMap = entity.GetProperties().AsMap()
	}
	if propMap == nil {
		propMap = make(map[string]any)
	}

	// Get existing labels or create new map
	labels, _ := propMap["labels"].(map[string]any)
	if labels == nil {
		labels = make(map[string]any)
	}

	// Merge config labels
	for key, value := range p.config.Labels {
		labels[key] = value
	}
	propMap["labels"] = labels

	// Set back
	newProps, err := structpb.NewStruct(propMap)
	if err != nil {
		return src, err
	}
	entity.Properties = newProps

	return models.NewRecord(entity, src.Edges()...), nil
}

func init() {
	if err := registry.Processors.Register("labels", func() plugins.Processor {
		return New(plugins.GetLog())
	}); err != nil {
		return
	}
}
