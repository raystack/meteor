package cmd

import (
	"github.com/odpf/meteor/generator"
	"github.com/spf13/cobra"
)

// GenCmd creates a command object for the "gen" action
func GenCmd() *cobra.Command {

	cmd := &cobra.Command{
		Use:   "gen",
		Short: "A collection of several useful generators.",
	}

	cmd.AddCommand(ExtCmd())

	return cmd
}

// ExtCmd creates a command object for the "version" action
func ExtCmd() *cobra.Command {

	var extType string

	cmd := &cobra.Command{
		Use:     "extractor [name]",
		Aliases: []string{"ext"},
		Args:    cobra.ExactValidArgs(1),
		Example: "meteor gen ext --type=topic",
		Short:   "Generate a new extractor",
		Run: func(cmd *cobra.Command, args []string) {
			generator.GenerateExtractor(extType, args[0])
		},
	}

	cmd.PersistentFlags().StringVar(&extType, "type", "", "type of extractor")
	cmd.MarkFlagRequired("extType")

	return cmd
}
