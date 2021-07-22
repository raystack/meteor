package cmd

import (
	goLog "log"

	"github.com/odpf/meteor/config"
	"github.com/odpf/meteor/core/recipe"
	"github.com/odpf/meteor/logger"
)

func run(recipeFile string) {
	c, err := config.LoadConfig()
	if err != nil {
		goLog.Fatal(err)
	}
	log := logger.New(c.LogLevel)

	reader := recipe.NewReader()
	rcp, err := reader.Read(recipeFile)
	if err != nil {
		log.Fatal(err)
	}
	runner, cleanFn := initRunner(c, log)
	defer cleanFn()

	_, err = runner.Run(rcp)
	if err != nil {
		log.Error(err)
	} else {
		log.Info("Done!")
	}
}
