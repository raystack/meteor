package cmd

import (
	"github.com/odpf/meteor/config"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/core/processor"
	"github.com/odpf/meteor/core/recipe"
	"github.com/odpf/meteor/core/sink"
	"github.com/odpf/meteor/metrics"
	"github.com/odpf/meteor/plugins"
	plugin "github.com/odpf/meteor/plugins/external"
	extractorPlugins "github.com/odpf/meteor/plugins/extractors"
	processorPlugins "github.com/odpf/meteor/plugins/processors"
	sinkPlugins "github.com/odpf/meteor/plugins/sinks"
)

func initRunner(config config.Config, logger plugins.Logger) (runner *recipe.Runner, cleanFn func()) {
	extractorFactory := initExtractorFactory(logger)
	processorFactory, killPluginsFn := initProcessorFactory()
	sinkFactory := initSinkFactory()
	metricsMonitor := initMetricsMonitor(config)
	runner = recipe.NewRunner(
		extractor.New(extractorFactory),
		processorFactory,
		sinkFactory,
		metricsMonitor,
	)
	cleanFn = func() {
		killPluginsFn()
	}
	return
}
func initExtractorFactory(logger plugins.Logger) *extractor.Factory {
	factory := extractor.NewFactory()
	extractorPlugins.PopulateFactory(factory, logger)
	return factory
}
func initProcessorFactory() (*processor.Factory, func()) {
	factory := processor.NewFactory()
	processorPlugins.PopulateFactory(factory)
	killPlugins, err := plugin.DiscoverPlugins(factory)
	if err != nil {
		panic(err)
	}

	return factory, killPlugins
}
func initSinkFactory() *sink.Factory {
	factory := sink.NewFactory()
	sinkPlugins.PopulateFactory(factory)
	return factory
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
