package cmd

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/meteor/agent"
	"github.com/odpf/meteor/config"
	"github.com/odpf/meteor/metrics"
	"github.com/odpf/meteor/recipe"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
	"github.com/odpf/salt/printer"
	"github.com/odpf/salt/term"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
)

// RunCmd creates a command object for the "run" action.
func RunCmd(lg log.Logger, mt *metrics.StatsdMonitor, cfg config.Config) *cobra.Command {
	return &cobra.Command{
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
			"group:core": "true",
		},
		RunE: func(cmd *cobra.Command, args []string) error {

			cs := term.NewColorScheme()
			runner := agent.NewAgent(agent.Config{
				ExtractorFactory:     registry.Extractors,
				ProcessorFactory:     registry.Processors,
				SinkFactory:          registry.Sinks,
				Monitor:              mt,
				Logger:               lg,
				MaxRetries:           cfg.MaxRetries,
				RetryInitialInterval: time.Duration(cfg.RetryInitialIntervalSeconds) * time.Second,
				StopOnSinkError:      cfg.StopOnSinkError,
			})

			recipes, err := recipe.NewReader().Read(args[0])
			if err != nil {
				return err
			}

			if len(recipes) == 0 {
				fmt.Println(cs.WarningIcon(), cs.Yellowf("No recipe found in [%s]", args[0]))
				return nil
			}

			report := [][]string{}
			var success = 0
			var failures = 0
			report = append(report, []string{"Status", "Recipe", "Source", "Duration(ms)", "Records"})

			bar := progressbar.NewOptions(len(recipes),
				progressbar.OptionEnableColorCodes(true),
				progressbar.OptionSetDescription("[cyan]running recipes [reset]"),
				progressbar.OptionShowCount(),
			)

			// Run recipes and collect results
			runs := runner.RunMultiple(recipes)
			for _, run := range runs {
				lg.Debug("recipe details", "recipe", run.Recipe)
				row := []string{}
				if run.Error != nil {
					lg.Error(run.Error.Error(), "recipe")
					failures++
					row = append(row, cs.FailureIcon(), run.Recipe.Name, cs.Grey(run.Recipe.Source.Type), cs.Greyf("%v ms", strconv.Itoa(run.DurationInMs)), cs.Greyf(strconv.Itoa(run.RecordCount)))
				} else {
					success++
					row = append(row, cs.SuccessIcon(), run.Recipe.Name, cs.Grey(run.Recipe.Source.Type), cs.Greyf("%v ms", strconv.Itoa(run.DurationInMs)), cs.Greyf(strconv.Itoa(run.RecordCount)))
				}
				report = append(report, row)
				err := bar.Add(1)
				if err != nil {
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
}
