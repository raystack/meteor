package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/raystack/meteor/agent"
	"github.com/raystack/meteor/config"
	"github.com/raystack/meteor/metrics"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/recipe"
	"github.com/raystack/meteor/registry"
	"github.com/raystack/salt/log"
	"github.com/raystack/salt/printer"
	"github.com/raystack/salt/term"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
)

// RunCmd creates a command object for the "run" action.
func RunCmd() *cobra.Command {
	var (
		report       [][]string
		pathToConfig string
		success      = 0
		failures     = 0
		configFile   string
	)

	cmd := &cobra.Command{
		Use:   "run <path>|<name>",
		Short: "Execute recipes for metadata extraction",
		Long: heredoc.Doc(`
			Execute specified recipes for metadata extraction.

			A recipe is a set of instructions and configurations defined by user, 
			and in Meteor they are used to define how metadata will be collected. 
			
			If a recipe file is provided, recipe will be executed as a single recipe.
			If a recipe directory is provided, recipes will be executed as a group of recipes.`),
		Example: heredoc.Doc(`
			$ meteor run recipe.yml

			# run all recipes in the specified directory
			$ meteor run _recipes/

			# run all recipes in the current directory
			$ meteor run .
		`),
		Args: cobra.ExactArgs(1),
		Annotations: map[string]string{
			"group": "core",
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(configFile)
			if err != nil {
				return err
			}

			lg := log.NewLogrus(log.LogrusWithLevel(cfg.LogLevel))
			plugins.SetLog(lg)

			// Monitoring system signals and creating context
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			var mts agent.Monitor

			if cfg.OtelEnabled {
				doneOtlp, err := metrics.InitOtel(ctx, cfg, lg, Version)
				if err != nil {
					return err
				}
				defer doneOtlp()

				mts = metrics.NewOtelMonitor()
			}

			runner := agent.NewAgent(agent.Config{
				ExtractorFactory:     registry.Extractors,
				ProcessorFactory:     registry.Processors,
				SinkFactory:          registry.Sinks,
				Monitor:              mts,
				Logger:               lg,
				MaxRetries:           cfg.MaxRetries,
				RetryInitialInterval: time.Duration(cfg.RetryInitialIntervalSeconds) * time.Second,
				StopOnSinkError:      cfg.StopOnSinkError,
				SinkBatchSize:        cfg.SinkBatchSize,
			})

			recipes, err := recipe.NewReader(lg, pathToConfig).Read(args[0])
			if err != nil {
				return err
			}

			if len(recipes) == 0 {
				fmt.Println(term.WarningIcon(), term.Yellowf("No recipe found in [%s]", args[0]))
				return nil
			}

			report = append(report, []string{"Status", "Recipe", "Source", "Duration(ms)", "Records"})

			bar := progressbar.NewOptions(len(recipes),
				progressbar.OptionEnableColorCodes(true),
				progressbar.OptionSetDescription("[cyan]running recipes [reset]"),
				progressbar.OptionShowCount(),
			)

			// Run recipes and collect results
			runs := runner.RunMultiple(ctx, recipes)
			for _, run := range runs {
				lg.Debug("recipe details", "recipe", run.Recipe)
				var row []string
				if run.Error != nil {
					lg.Error(run.Error.Error(), "recipe", run.Recipe.Name)
					failures++
					row = append(row, term.FailureIcon(), run.Recipe.Name, term.Grey(run.Recipe.Source.Name), term.Greyf("%v ms", strconv.Itoa(run.DurationInMs)), term.Greyf("%s", strconv.Itoa(run.RecordCount)))
				} else {
					success++
					row = append(row, term.SuccessIcon(), run.Recipe.Name, term.Grey(run.Recipe.Source.Name), term.Greyf("%v ms", strconv.Itoa(run.DurationInMs)), term.Greyf("%s", strconv.Itoa(run.RecordCount)))
				}
				report = append(report, row)
				if err = bar.Add(1); err != nil {
					return err
				}
			}

			// Print the report
			if failures > 0 {
				fmt.Println("\nSome recipes were not successful")
			} else {
				fmt.Println("\nAll recipes ran successful")
			}
			fmt.Printf("%d failing, %d successful, and %d total\n\n", failures, success, len(recipes))
			printer.Table(os.Stdout, report)
			return nil
		},
	}

	cmd.Flags().StringVar(&pathToConfig, "var", "", "Path to Config file with env variables for recipe")
	cmd.Flags().StringVarP(&configFile, "config", "c", "./meteor.yaml", "file path for agent level config")

	return cmd
}
