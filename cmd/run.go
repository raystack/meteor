package cmd

import (
	"fmt"

	"github.com/odpf/meteor/agent"
	"github.com/odpf/meteor/internal/metrics"
	"github.com/odpf/meteor/recipe"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
)

// RunCmd creates a command object for the "run" action.
func RunCmd(lg log.Logger, mt *metrics.StatsdMonitor) *cobra.Command {
	return &cobra.Command{
		Use:     "run [COMMAND]",
		Example: "meteor run recipe.yaml",
		Short:   "Run meteor for provided recipes.",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {

			runner := agent.NewAgent(registry.Extractors, registry.Processors, registry.Sinks, mt)

			reader := recipe.NewReader()
			recipes, err := reader.Read(args[0])
			if err != nil {
				return err
			}

			count := len(recipes)

			if count == 0 {
				lg.Info(fmt.Sprintf("no recipe found for path: %s", args[0]))
				return nil
			}

			runs := runner.RunMultiple(recipes)

			for _, run := range runs {
				if run.Error != nil {
					lg.Error(fmt.Sprintf("%s: %s", run.Recipe.Name, run.Error))
					continue
				}
				lg.Info(fmt.Sprintf("%s: success", run.Recipe.Name))
			}
			return nil
		},
	}
}
