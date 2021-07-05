package cmd

import (
	"log"

	"github.com/odpf/meteor/config"
	"github.com/odpf/meteor/recipes"
)

func run(recipeFile string) {
	c, err := config.LoadConfig()
	if err != nil {
		log.Fatal(err)
	}

	runner := initRunner(c)
	reader := recipes.NewReader()
	recipe, err := reader.Read(recipeFile)
	if err != nil {
		log.Fatal(err)
	}
	_, err = runner.Run(recipe)
	if err != nil {
		log.Fatal(err)
	}
}
