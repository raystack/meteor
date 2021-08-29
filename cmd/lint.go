package cmd

import (
	"fmt"

	"github.com/odpf/salt/log"

	"github.com/spf13/cobra"
)

// LintCmd creates a command object for linting recipes
func LintCmd(lg log.Logger) *cobra.Command {

	cmd := &cobra.Command{
		Use:     "lint [path]",
		Aliases: []string{"l"},
		Args:    cobra.ExactValidArgs(1),
		Example: "meteor lint recipe.yaml",
		Short:   "Validate a receipe",
		Annotations: map[string]string{
			"group:core": "true",
		},
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Validating recipe...")
		},
	}
	return cmd
}
