package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var root = &cobra.Command{
	Use:   "meteor",
	Short: "Meteor is a metadata collector tool",
	Long: `Meteor is a metadata collector tool that helps to extract and sink 
		   metadata from the source (e.g. DB, kafka, etc) and sink them to the destination (e.g. kafka, http).`,
}

// Execute executes the root command
func Execute() {

	root.AddCommand(RunCmd())
	root.AddCommand(GenCmd())

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
