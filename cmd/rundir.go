package cmd

import (
	"github.com/odpf/meteor/plugins"
	goLog "log"

	"github.com/odpf/meteor/config"
	"github.com/odpf/meteor/core/recipe"
	"github.com/odpf/meteor/logger"
)

// run recipes in a given directory path
func rundir(dirPath string) {
	c, err := config.LoadConfig()
	if err != nil {
		goLog.Fatal(err)
	}
	log := logger.New(c.LogLevel)
	plugins.Log.Level = log.Level

	reader := recipe.NewReader()
	recipeList, err := reader.ReadDir(dirPath)
	if err != nil {
		log.Fatal(err)
	}

	failedRecipes, err := initRunner(c, log).RunMultiple(recipeList)
	if err != nil {
		log.Fatal(err)
	}
	log.WithField("failed_recipes", failedRecipes).Info("Done!")
}
