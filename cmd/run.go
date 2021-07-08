package cmd

import (
	"github.com/odpf/meteor/config"
	"github.com/odpf/meteor/recipes"
)

func run(recipeFile string) {
	c, err := config.LoadConfig()
	if err != nil {
		panic(err)
	}

	runner, cleanFn := initRunner(c)
	defer cleanFn()
	reader := recipes.NewReader()
	recipe, err := reader.Read(recipeFile)
	if err != nil {
		panic(err)
	}
	_, err = runner.Run(recipe)
	if err != nil {
		panic(err)
	}
}
