package cmd

import (
	"strings"

	"github.com/odpf/meteor/generator"
	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
)

// GenCmd creates a command object for the "gen" action
func GenCmd(lg log.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "gen",
		Short: "Generate sample recipes and plugins.",
		Annotations: map[string]string{
			"group:core": "true",
		},
	}

	cmd.AddCommand(GenRecipeCmd())

	return cmd
}

// GenRecipeCmd creates a command object for generating recipes
func GenRecipeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "recipe [name]",
		Aliases: []string{"r"},
		Args:    cobra.ExactValidArgs(1),
		Example: "meteor gen recipe sample -e bigquery -s console,kafka -p enrich",
		Short:   "Generate a new recipe",
		Annotations: map[string]string{
			"group:core": "true",
		},
		Run: func(cmd *cobra.Command, args []string) {

			ext, _ := cmd.Flags().GetString("extractor")
			s, _ := cmd.Flags().GetString("sinks")
			p, _ := cmd.Flags().GetString("processors")

			sinks := strings.Split(s, ",")
			processors := strings.Split(p, ",")

			generator.Recipe(args[0], ext, sinks, processors)
		},
	}

	cmd.PersistentFlags().StringP("extraactor", "e", "", "Type of extractor")
	cmd.PersistentFlags().StringP("sinks", "s", "", "List of sinks types")
	cmd.PersistentFlags().StringP("processors", "p", "", "List of processor types")

	return cmd
}
