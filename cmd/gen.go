package cmd

import (
	"strings"

	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/meteor/generator"
	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
)

// GenCmd creates a command object for the "gen" action
func GenCmd(lg log.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "gen",
		Short: "Generate sample recipes and plugins",
		Annotations: map[string]string{
			"group:core": "true",
		},
	}

	cmd.AddCommand(GenRecipeCmd())

	return cmd
}

// GenRecipeCmd creates a command object for generating recipes
func GenRecipeCmd() *cobra.Command {
	var (
		extractor  string
		sinks      string
		processors string
	)

	cmd := &cobra.Command{
		Use:     "recipe [name]",
		Aliases: []string{"r"},
		Args:    cobra.ExactValidArgs(1),
		Short:   "Generate a new recipe",
		Long: heredoc.Doc(`
			Generate a new recipe.

			The recipe will be printed on standard output.
			Specify recipe name with the first argument without extension.
			Use commma to separate multiple sinks and processors.
		`),
		Example: heredoc.Doc(`
			# generate a recipe with a bigquery extractor and a console sink
			$ meteor gen recipe sample -e bigquery -s console

			# generate recipe with multiple sinks
			$ meteor gen recipe sample -e bigquery -s columbus,kafka -p enrich

			# store recipe to a file
			$ meteor gen recipe sample -e bigquery -s columbus > recipe.yaml
		`),
		Annotations: map[string]string{
			"group:core": "true",
		},
		RunE: func(cmd *cobra.Command, args []string) error {

			var sinkList []string
			var procList []string

			if sinks != "" {
				sinkList = strings.Split(sinks, ",")
			}

			if processors != "" {
				procList = strings.Split(processors, ",")
			}

			return generator.Recipe(args[0], extractor, sinkList, procList)
		},
	}

	cmd.Flags().StringVarP(&extractor, "extractor", "e", "", "Type of extractor")
	cmd.Flags().StringVarP(&sinks, "sinks", "s", "", "List of sink types")
	cmd.Flags().StringVarP(&processors, "processors", "p", "", "List of processor types")

	return cmd
}
