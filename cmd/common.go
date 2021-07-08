package cmd

import (
	"log"

	"github.com/odpf/meteor/config"
	"github.com/odpf/meteor/extractors"
	"github.com/odpf/meteor/metrics"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/processors"
	"github.com/odpf/meteor/recipes"
	"github.com/odpf/meteor/sinks"
)

func initRunner(config config.Config) (runner *recipes.Runner, cleanFn func()) {
	extractorStore := initExtractorStore()
	processorStore, killPluginsFn := initProcessorStore()
	sinkStore := initSinkStore()
	metricsMonitor := initMetricsMonitor(config)
	runner = recipes.NewRunner(
		extractorStore,
		processorStore,
		sinkStore,
		metricsMonitor,
	)
	cleanFn = func() {
		killPluginsFn()
	}
	return
}
func initExtractorStore() *extractors.Store {
	store := extractors.NewStore()
	extractors.PopulateStore(store)
	return store
}
func initProcessorStore() (*processors.Store, func()) {
	store := processors.NewStore()
	processors.PopulateStore(store)
	killPlugins, err := plugins.DiscoverPlugins(store)
	if err != nil {
		panic(err)
	}

	return store, killPlugins
}
func initSinkStore() *sinks.Store {
	store := sinks.NewStore()
	sinks.PopulateStore(store)
	return store
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
