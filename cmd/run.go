package cmd

import (
	"github.com/odpf/meteor/config"
	"github.com/odpf/meteor/extractors"
	"github.com/odpf/meteor/metrics"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/processors"
	"github.com/odpf/meteor/recipes"
	"github.com/odpf/meteor/sinks"
)

func run(recipeFile string) {
	c, err := config.LoadConfig()
	if err != nil {
		panic(err)
	}

	extractorStore := initExtractorStore()
	sinkStore := initSinkStore()
	metricsMonitor := initMetricsMonitor(c)
	processorStore, killPlugins := initProcessorStore()
	defer killPlugins()
	recipeRunner := recipes.NewRunner(
		extractorStore,
		processorStore,
		sinkStore,
		metricsMonitor,
	)

	recipeReader := recipes.NewReader()
	recipe, err := recipeReader.Read(recipeFile)
	if err != nil {
		panic(err)
	}
	_, err = recipeRunner.Run(recipe)
	if err != nil {
		panic(err)
	}

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
		panic(err)
	}
	monitor := metrics.NewStatsdMonitor(client, c.StatsdPrefix)
	return monitor
}
