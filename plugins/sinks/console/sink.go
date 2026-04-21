package console

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/MakeNowJust/heredoc"
	"github.com/raystack/meteor/models"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
)

//go:embed README.md
var summary string

type Config struct {
	Format string `mapstructure:"format" default:"json" validate:"omitempty,oneof=json markdown"`
}

var info = plugins.Info{
	Description: "Log metadata to standard output.",
	Summary:     summary,
	Tags:        []string{"oss", "debug"},
	SampleConfig: heredoc.Doc(`
		format: json
	`),
}

type Sink struct {
	plugins.BasePlugin
	logger log.Logger
	config Config
}

func New(logger log.Logger) plugins.Syncer {
	s := &Sink{
		logger: logger,
	}
	s.BasePlugin = plugins.NewBasePlugin(info, &s.config)

	return s
}

func (s *Sink) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = s.BasePlugin.Init(ctx, config); err != nil {
		return err
	}
	return
}

func (s *Sink) Sink(ctx context.Context, batch []models.Record) (err error) {
	for i, record := range batch {
		switch s.config.Format {
		case "markdown":
			if i > 0 {
				fmt.Print("\n---\n\n")
			}
			md, err := models.RecordToMarkdown(record)
			if err != nil {
				return err
			}
			fmt.Print(string(md))
		default:
			jsonBytes, err := models.RecordToJSON(record)
			if err != nil {
				return err
			}
			fmt.Println(string(jsonBytes))
		}
	}
	return nil
}

func (s *Sink) Close() (err error) { return }

func init() {
	if err := registry.Sinks.Register("console", func() plugins.Syncer {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
