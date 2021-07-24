package cmd

import (
	"github.com/odpf/meteor/config"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/core/processor"
	"github.com/odpf/meteor/core/recipe"
	"github.com/odpf/meteor/core/sink"
	"github.com/odpf/meteor/metrics"
	"github.com/odpf/meteor/plugins"
)

func initRunner(config config.Config, logger plugins.Logger) (runner *recipe.Runner) {
	metricsMonitor := initMetricsMonitor(config)
	runner = recipe.NewRunner(
		extractor.Catalog,
		processor.Catalog,
		sink.Catalog,
		metricsMonitor,
	)
	return
}

func initMetricsMonitor(c config.Config) *metrics.StatsdMonitor {
	if !c.StatsdEnabled {
		return nil
	}

	client, err := metrics.NewStatsdClient(c.StatsdHost)
	if err != nil {
		panic(err)
	}
	monitor := metrics.NewStatsdMonitor(client, c.StatsdPrefix)
	return monitor
}
