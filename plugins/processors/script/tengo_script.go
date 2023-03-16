package script

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"

	"github.com/MakeNowJust/heredoc"
	"github.com/d5/tengo/v2"
	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/internal/tengoutil"
	"github.com/goto/meteor/plugins/internal/tengoutil/structmap"
	"github.com/goto/meteor/registry"
	"github.com/goto/salt/log"
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

// Processor executes the configured Tengo script to transform the given asset
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
	  asset.owners = append(asset.owners || [], { name: "Big Mom", email: "big.mom@wholecakeisland.com" })
`)

var info = plugins.Info{
	Description:  "Transform the asset with a Tengo script",
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

	s, err := tengoutil.NewSecureScript(([]byte)(p.config.Script), map[string]interface{}{
		"asset": map[string]interface{}{},
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
	m, err := structmap.AsMap(src.Data())
	if err != nil {
		return models.Record{}, fmt.Errorf("script processor: %w", err)
	}

	c := p.compiled.Clone()
	if err := c.Set("asset", m); err != nil {
		return models.Record{}, fmt.Errorf("script processor: set asset into vm: %w", err)
	}

	if err := c.RunContext(ctx); err != nil {
		return models.Record{}, fmt.Errorf("script processor: run script: %w", err)
	}

	var transformed *v1beta2.Asset
	if err := structmap.AsStruct(c.Get("asset").Map(), &transformed); err != nil {
		return models.Record{}, fmt.Errorf("script processor: overwrite asset: %w", err)
	}

	return models.NewRecord(transformed), nil
}
