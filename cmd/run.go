package cmd

import (
	"fmt"
	"os"
	"strconv"

	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/meteor/agent"
	"github.com/odpf/meteor/metrics"
	"github.com/odpf/meteor/recipe"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
	"github.com/odpf/salt/printer"
	"github.com/odpf/salt/term"
	"github.com/spf13/cobra"
)

// RunCmd creates a command object for the "run" action.
func RunCmd(lg log.Logger, mt *metrics.StatsdMonitor) *cobra.Command {
	return &cobra.Command{
		Use:   "run <path>|<name>",
		Short: "Execute recipes for metadata extraction",
		Long: heredoc.Doc(`
			Execute specified recipes for metadata extraction.

			A recipe is a set of instructions and configurations defined by user, 
			and in Meteor they are used to define how metadata will be collected. 
			
			If a recipe file is provided, recipe will be 
			executed as a single recipe.
			If a recipe directory is provided, recipes will 
			be executed as a group of recipes.
		`),
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
			runner := agent.NewAgent(registry.Extractors, registry.Processors, registry.Sinks, mt, lg)

			recipes, err := recipe.NewReader().Read(args[0])
			if err != nil {
				return err
			}

			if len(recipes) == 0 {
				fmt.Println(cs.WarningIcon(), cs.Yellowf(" no recipe found in [%s]", args[0]))
				return nil
			}

			var success = 0
			var failures = 0
			tabular_report := [][]string{}
			tabular_report = append(tabular_report, []string{"Recipe", "Source", "Status", "Duration(ms)"})
			runs := runner.RunMultiple(recipes)
			for _, run := range runs {
				lg.Debug("recipe details", "recipe", run.Recipe)
				report_row := []string{run.Recipe.Name, run.Recipe.Source.Type}

				if run.Error != nil {
					lg.Error(run.Error.Error(), "recipe")
					failures++
					report_row = append(report_row, cs.FailureIcon()+cs.Redf("failure"))
				} else {
					success++
					report_row = append(report_row, cs.SuccessIcon()+cs.Greenf("successful"))
				}

				report_row = append(report_row, strconv.Itoa(run.DurationInMs))
				tabular_report = append(tabular_report, report_row)
			}

			fmt.Print("\n\n")
			printer.Table(os.Stdout, tabular_report)
			fmt.Printf("Total: %d, Success: %d, Failures: %d\n", len(recipes), success, failures)

			return nil
		},
	}
}
