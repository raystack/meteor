package cmd

import (
	"fmt"

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
		Example: "meteor lint recipe.yaml",
		Short:   "Validate recipes.",
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
				return nil
			}

			for _, recipe := range recipes {
				if err := runner.Validate(recipe); err != nil {
					lg.Error(err.Error())
					continue
				}
			}
			return nil
		},
	}
}
