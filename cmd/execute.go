package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "meteor",
	Short: "Meteor is a metadata collector tool",
	Long: `Meteor is a metadata collector tool that helps to extract and sink 
	metadata from the source (e.g. DB, kafka, etc) and sink them to the destination (e.g. kafka, http).`,
}
var runCmd = &cobra.Command{
	Use:   "run [recipe-file]",
	Short: "Run a single recipe file",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		run(args[0])
	},
}

func Execute() {
	rootCmd.AddCommand(
		runCmd,
	)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
