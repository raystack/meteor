package script

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"

	"github.com/MakeNowJust/heredoc"
	"github.com/d5/tengo/v2"
	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/internal/tengoutil"
	"github.com/raystack/meteor/plugins/internal/tengoutil/structmap"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
)

func init() {
	if err := registry.Processors.Register("script", func() plugins.Processor {
		return New(plugins.GetLog())
	}); err != nil {
		return
	}
}

//go:embed README.md
var summary string

type Config struct {
	Engine string `mapstructure:"engine" validate:"required,oneof=tengo"`
	Script string `mapstructure:"script" validate:"required"`
}

// Processor executes the configured Tengo script to transform the given entity
// record.
type Processor struct {
	plugins.BasePlugin
	config Config
	logger log.Logger

	compiled *tengo.Compiled
}

var sampleConfig = heredoc.Doc(`
	engine: tengo
	script: |
	  entity.name = entity.name + " (modified)"
`)

var info = plugins.Info{
	Description:  "Transform the entity with a Tengo script",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"processor", "transform", "script"},
}

// New create a new processor
func New(logger log.Logger) *Processor {
	p := &Processor{
		logger: logger,
	}
	p.BasePlugin = plugins.NewBasePlugin(info, &p.config)

	return p
}

func (p *Processor) Init(ctx context.Context, config plugins.Config) error {
	if err := p.BasePlugin.Init(ctx, config); err != nil {
		return fmt.Errorf("script processor init: %w", err)
	}

	s, err := tengoutil.NewSecureScript(([]byte)(p.config.Script), map[string]any{
		"entity": map[string]any{},
	})
	if err != nil {
		return fmt.Errorf("script processor init: %w", err)
	}

	compiled, err := s.Compile()
	if err != nil {
		return fmt.Errorf("script processor init: compile script: %w", err)
	}

	p.compiled = compiled

	return nil
}

// Process processes the data
func (p *Processor) Process(ctx context.Context, src models.Record) (models.Record, error) {
	m, err := structmap.AsMap(src.Entity())
	if err != nil {
		return models.Record{}, fmt.Errorf("script processor: %w", err)
	}

	entityMap, ok := m.(map[string]any)
	if !ok {
		return models.Record{}, fmt.Errorf("script processor: expected map[string]interface{}, got %T", m)
	}

	c := p.compiled.Clone()
	if err := c.Set("entity", entityMap); err != nil {
		return models.Record{}, fmt.Errorf("script processor: set entity into vm: %w", err)
	}

	if err := c.RunContext(ctx); err != nil {
		return models.Record{}, fmt.Errorf("script processor: run script: %w", err)
	}

	// Merge the result back into the original map.
	// Tengo returns only modified fields from an ImmutableMap, so we merge
	// the script output on top of the original to preserve unmodified fields.
	resultMap := c.Get("entity").Map()
	for k, v := range resultMap {
		entityMap[k] = v
	}

	var transformed *meteorv1beta1.Entity
	if err := structmap.AsStruct(entityMap, &transformed); err != nil {
		return models.Record{}, fmt.Errorf("script processor: overwrite entity: %w", err)
	}

	return models.NewRecord(transformed, src.Edges()...), nil
}
