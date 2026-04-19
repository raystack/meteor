package console

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/raystack/meteor/models"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
)

//go:embed README.md
var summary string

var info = plugins.Info{
	Description:  "Log to standard output",
	Summary:      summary,
	Tags:         []string{"oss", "debug"},
	SampleConfig: "",
}

type Sink struct {
	plugins.BasePlugin
	logger log.Logger
}

func New(logger log.Logger) plugins.Syncer {
	s := &Sink{
		logger: logger,
	}
	s.BasePlugin = plugins.NewBasePlugin(info, nil)

	return s
}

func (s *Sink) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = s.BasePlugin.Init(ctx, config); err != nil {
		return err
	}
	return
}

func (s *Sink) Sink(ctx context.Context, batch []models.Record) (err error) {
	for _, record := range batch {
		jsonBytes, err := models.RecordToJSON(record)
		if err != nil {
			return err
		}
		fmt.Println(string(jsonBytes))
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
