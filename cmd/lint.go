package cmd

import (
	"fmt"

	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/meteor/agent"
	"github.com/odpf/meteor/metrics"
	"github.com/odpf/meteor/recipe"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
	"github.com/odpf/salt/term"

	"github.com/spf13/cobra"
)

// LintCmd creates a command object for linting recipes
func LintCmd(lg log.Logger, mt *metrics.StatsdMonitor) *cobra.Command {
	return &cobra.Command{
		Use:     "lint [path]",
		Aliases: []string{"l"},
		Args:    cobra.ExactValidArgs(1),
		Short:   "Check for issues in recipes",
		Long: heredoc.Doc(`
			Check for issues specified recipes.

			Linters are run on the recipe files in the specified path.
			If no path is specified, the current directory is used.
		`),
		Example: heredoc.Doc(`
			$ meteor lint recipe.yml

			# lint all recipes in the specified directory
			$ meteor lint _recipes/

			# lint all recipes in the current directory
			$ meteor lint .
		`),
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
				fmt.Println(cs.Yellowf("no recipe found in [%s]", args[0]))
				fmt.Println(cs.Blue("\nUse 'meteor gen recipe' to generate a new recipe."))
				return nil
			}

			report := []string{""}
			var success = 0
			var failures = 0
			for _, recipe := range recipes {
				errs := runner.Validate(recipe)
				if len(errs) > 0 {
					for _, err := range errs {
						lg.Error(err.Error())
					}
					report = append(report, fmt.Sprint(cs.FailureIcon(), cs.Redf(" %d errors found is recipe %s", len(errs), recipe.Name)))
					failures++
					continue
				}
				report = append(report, fmt.Sprint(cs.SuccessIcon(), cs.Greenf(" 0 error found in recipe %s", recipe.Name)))
				success++
			}

			for _, line := range report {
				fmt.Println(line)
			}
			fmt.Printf("Total: %d, Success: %d, Failures: %d\n", len(recipes), success, failures)
			return nil
		},
	}
}
