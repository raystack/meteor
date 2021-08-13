package cmd

import (
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

			if len(recipes) == 1 {
				run := runner.Run(recipes[0])
				if run.Error != nil {
					return run.Error
				}
				lg.Info("Done!", run)
			} else {
				runs := runner.RunMultiple(recipes)
				lg.Info("Done!", runs)
			}

			return nil
		},
	}
}
