package cmd

import (
	"github.com/odpf/meteor/agent"
	"github.com/odpf/meteor/metrics"
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
		Annotations: map[string]string{
			"group:core": "true",
		},
		RunE: func(cmd *cobra.Command, args []string) error {

			runner := agent.NewAgent(registry.Extractors, registry.Processors, registry.Sinks, mt)

			recipes, err := recipe.NewReader().Read(args[0])
			if err != nil {
				return err
			}

			if len(recipes) == 0 {
				lg.Info("no recipe found", "path", args[0])
				return nil
			}

			runs := runner.RunMultiple(recipes)
			for _, run := range runs {
				lg.Debug("recipe details", "recipe", run.Recipe)
				if run.Error != nil {
					lg.Error(run.Error.Error(), "recipe", run.Recipe.Name)
					continue
				}
				lg.Info("success", "recipe", run.Recipe.Name)
			}
			return nil
		},
	}
}
