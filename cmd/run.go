package cmd

import (
	"log"

	"github.com/odpf/meteor/config"
	"github.com/odpf/meteor/domain"
	"github.com/odpf/meteor/extractors"
	"github.com/odpf/meteor/processors"
	"github.com/odpf/meteor/services"
	"github.com/odpf/meteor/sinks"
	"github.com/odpf/meteor/stores"
)

func Run() {
	var err error

	config, err := config.LoadConfig()
	if err != nil {
		log.Fatal(err)
	}
	recipeStore := initRecipeStore(config.RecipeStorageURL)
	extractorStore := initExtractorStore()
	processorStore := initProcessorStore()
	sinkStore := initSinkStore()
	recipeService := services.NewRecipeService(
		recipeStore,
		extractorStore,
		processorStore,
		sinkStore,
	)

	recipe, err := recipeService.ReadFromFile("./sample-recipe.yaml")
	if err != nil {
		log.Fatal(err)
	}
	_, err = recipeService.Run(recipe)
	if err != nil {
		log.Fatal(err)
	}
}
func initRecipeStore(recipeStorageURL string) domain.RecipeStore {
	store, err := stores.NewRecipeStore(recipeStorageURL)
	if err != nil {
		log.Fatal(err.Error())
	}

	return store
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
