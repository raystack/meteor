package cmd

import (
	"github.com/odpf/meteor/generator"
	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
)

// GenCmd creates a command object for the "gen" action
func GenCmd(lg log.Logger) *cobra.Command {

	cmd := &cobra.Command{
		Use:   "gen",
		Short: "A collection of several useful generators.",
		Annotations: map[string]string{
			"group:core": "true",
		},
	}

	cmd.AddCommand(ExtCmd())

	return cmd
}

// ExtCmd creates a command object for generating recipes
func ExtCmd() *cobra.Command {

	cmd := &cobra.Command{
		Use:     "recipe [name]",
		Aliases: []string{"r"},
		Args:    cobra.ExactValidArgs(1),
		Example: "meteor recipe ext --type=topic",
		Short:   "Generate a new extractor",
		Run: func(cmd *cobra.Command, args []string) {
			generator.Recipe(args[0])
		},
	}
	return cmd
}
