package cmd

import (
	"fmt"
	"log"
	"net/http"

	"github.com/odpf/meteor/api"
	"github.com/odpf/meteor/config"
	"github.com/odpf/meteor/domain"
	"github.com/odpf/meteor/extractors"
	"github.com/odpf/meteor/processors"
	"github.com/odpf/meteor/secrets"
	"github.com/odpf/meteor/services"
	"github.com/odpf/meteor/sinks"
	"github.com/odpf/meteor/stores"
)

func Serve() {
	var err error

	config, err := config.LoadConfig()
	if err != nil {
		log.Fatal(err)
	}
	recipeStore := initRecipeStore(config.RecipeStorageURL)
	extractorStore := initExtractorStore()
	processorStore := initProcessorStore()
	sinkStore := initSinkStore()
	secretStore := initSecretStore(config.SecretStorageURL)
	recipeService := services.NewRecipeService(
		recipeStore,
		extractorStore,
		processorStore,
		sinkStore,
		secretStore,
	)

	recipeHandler := api.NewRecipeHandler(recipeService)
	secretHandler := api.NewSecretHandler(secretStore)
	router := api.NewRouter()
	api.SetupRoutes(router, recipeHandler, secretHandler)

	fmt.Println("Listening on port :" + config.Port)
	err = http.ListenAndServe(":"+config.Port, router)
	if err != nil {
		fmt.Println(err)
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
func initSecretStore(secretStorageURL string) secrets.Store {
	store, err := stores.NewSecretStore(secretStorageURL)
	if err != nil {
		log.Fatal(err.Error())
	}

	return store
}
