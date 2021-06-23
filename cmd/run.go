package cmd

import (
	"errors"
	"log"
	"os"

	"github.com/odpf/meteor/extractors"
	"github.com/odpf/meteor/processors"
	"github.com/odpf/meteor/recipes"
	"github.com/odpf/meteor/sinks"
)

func Run() {
	var err error

	extractorStore := initExtractorStore()
	processorStore := initProcessorStore()
	sinkStore := initSinkStore()
	recipeRunner := recipes.NewRunner(
		extractorStore,
		processorStore,
		sinkStore,
	)
	recipeReader := recipes.NewReader()
	recipe, err := recipeReader.Read(readPathFromConsole())
	if err != nil {
		log.Fatal(err)
	}
	_, err = recipeRunner.Run(recipe)
	if err != nil {
		log.Fatal(err)
	}
}
func readPathFromConsole() string {
	args := os.Args
	if len(args) < 3 {
		err := errors.New("path missing")
		log.Fatal(err)
	}
	return args[2]
}
func initExtractorStore() *extractors.Store {
	store := extractors.NewStore()
	extractors.PopulateStore(store)
	return store
}
func initProcessorStore() *processors.Store {
	store := processors.NewStore()
	processors.PopulateStore(store)
	return store
}
func initSinkStore() *sinks.Store {
	store := sinks.NewStore()
	sinks.PopulateStore(store)
	return store
}
