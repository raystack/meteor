package cmd

import (
	"github.com/odpf/meteor/config"
	"github.com/odpf/meteor/core/recipe"
)

func run(recipeFile string) {
	c, err := config.LoadConfig()
	if err != nil {
		panic(err)
	}

	runner, cleanFn := initRunner(c)
	defer cleanFn()
	reader := recipe.NewReader()
	rcp, err := reader.Read(recipeFile)
	if err != nil {
		panic(err)
	}
	_, err = runner.Run(rcp)
	if err != nil {
		panic(err)
	}
}
