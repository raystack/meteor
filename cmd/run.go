package cmd

import (
	"github.com/odpf/meteor/config"
	"github.com/odpf/meteor/core/recipe"
	"github.com/odpf/meteor/logger"
)

func run(recipeFile string) {
	c, err := config.LoadConfig()
	if err != nil {
		panic(err)
	}
	log := logger.New(c.LogLevel)

	runner, cleanFn := initRunner(c, log)
	defer cleanFn()
	reader := recipe.NewReader()
	rcp, err := reader.Read(recipeFile)
	if err != nil {
		panic(err)
	}
	_, err = runner.Run(rcp)
	if err != nil {
		log.Error(err)
	} else {
		log.Info("Done!")
	}
}
