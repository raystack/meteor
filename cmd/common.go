package cmd

import (
	"log"

	"github.com/odpf/meteor/config"
	"github.com/odpf/meteor/extractors"
	"github.com/odpf/meteor/metrics"
	pkgExtractors "github.com/odpf/meteor/pkg/extractors"
	pkgProcessors "github.com/odpf/meteor/pkg/processors"
	pkgSinks "github.com/odpf/meteor/pkg/sinks"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/processors"
	"github.com/odpf/meteor/recipes"
	"github.com/odpf/meteor/sinks"
)

func initRunner(config config.Config) (runner *recipes.Runner, cleanFn func()) {
	extractorFactory := initExtractorFactory()
	processorFactory, killPluginsFn := initProcessorFactory()
	sinkFactory := initSinkFactory()
	metricsMonitor := initMetricsMonitor(config)
	runner = recipes.NewRunner(
		extractorFactory,
		processorFactory,
		sinkFactory,
		metricsMonitor,
	)
	cleanFn = func() {
		killPluginsFn()
	}
	return
}
func initExtractorFactory() *extractors.Factory {
	factory := extractors.NewFactory()
	pkgExtractors.PopulateFactory(factory)
	return factory
}
func initProcessorFactory() (*processors.Factory, func()) {
	factory := processors.NewFactory()
	pkgProcessors.PopulateFactory(factory)
	killPlugins, err := plugins.DiscoverPlugins(factory)
	if err != nil {
		panic(err)
	}

	return factory, killPlugins
}
func initSinkFactory() *sinks.Factory {
	factory := sinks.NewFactory()
	pkgSinks.PopulateFactory(factory)
	return factory
}
func initMetricsMonitor(c config.Config) *metrics.StatsdMonitor {
	if !c.StatsdEnabled {
		return nil
	}

	client, err := metrics.NewStatsdClient(c.StatsdHost)
	if err != nil {
		log.Fatal(err)
	}
	monitor := metrics.NewStatsdMonitor(client, c.StatsdPrefix)
	return monitor
}
