package cmd

import (
	"github.com/odpf/meteor/plugins"
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
	plugins.Log.Level = log.Level

	reader := recipe.NewReader()
	rcp, err := reader.Read(recipeFile)
	if err != nil {
		log.Fatal(err)
	}
	if err = initRunner(c, log).Run(rcp); err != nil {
		log.Fatal(err)
	}
	log.Info("Done!")
}
