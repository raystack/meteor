package cmd

import (
	"fmt"
	"log"
	"os"

	"github.com/odpf/meteor/config"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/core/processor"
	"github.com/odpf/meteor/core/recipe"
	"github.com/odpf/meteor/core/sink"
	"github.com/odpf/meteor/logger"
	"github.com/odpf/meteor/metrics"
	"github.com/odpf/meteor/plugins"
	"github.com/spf13/cobra"
)

// RunCmd creates a command object for the "run" action
func RunCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "run [COMMAND]",
		Short:   "Run meteor for provided receipes",
		Example: "meteor run recipe.yaml",
		Args:    cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {

			fi, err := os.Stat(args[0])
			if err != nil {
				fmt.Println(err)
				return
			}
			switch mode := fi.Mode(); {
			case mode.IsDir():
				rundir(args[0])
			case mode.IsRegular():
				run(args[0])
			}
		},
	}

	return cmd
}

// Run a single recipe
func run(recipeFile string) {
	c, err := config.LoadConfig()
	if err != nil {
		log.Fatal(err)
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
	log.Println("! Done")
}

// Run a directory of recipes
func rundir(dirPath string) {
	c, err := config.LoadConfig()
	if err != nil {
		log.Fatal(err)
	}

	reader := recipe.NewReader()
	recipeList, err := reader.ReadDir(dirPath)
	if err != nil {
		log.Fatal(err)
	}

	log := logger.New(c.LogLevel)
	plugins.Log.Level = log.Level

	failedRecipes, err := initRunner(c, log).RunMultiple(recipeList)
	if err != nil {
		log.Fatal(err)
	}
	log.WithField("failed_recipes", failedRecipes).Info("Done!")
}

func initRunner(config config.Config, logger plugins.Logger) (runner *recipe.Runner) {
	metricsMonitor := initMetricsMonitor(config)
	runner = recipe.NewRunner(
		extractor.Catalog,
		processor.Catalog,
		sink.Catalog,
		metricsMonitor,
	)
	return
}

func initMetricsMonitor(c config.Config) *metrics.StatsdMonitor {
	if !c.StatsdEnabled {
		return nil
	}

	client, err := metrics.NewStatsdClient(c.StatsdHost)
	if err != nil {
		panic(err)
	}
	monitor := metrics.NewStatsdMonitor(client, c.StatsdPrefix)
	return monitor
}
