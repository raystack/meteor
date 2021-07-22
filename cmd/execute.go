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
var rundirCmd = &cobra.Command{
	Use:   "rundir [directory-path]",
	Short: "Run recipes inside a directory",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		rundir(args[0])
	},
}

func generateExtractorFun() *cobra.Command {
	var extractorType string
	var extractorName string

	var generateExtractorCmd = &cobra.Command{
		Use:   "rungen [type]",
		Short: "Scaffold a extractor file",
		// Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			generateExtractor(extractorType, extractorName)
		},
	}
	generateExtractorCmd.PersistentFlags().StringVar(&extractorType, "type", "", "type of extractor [table,topic]")
	generateExtractorCmd.MarkFlagRequired("extractorType")
	generateExtractorCmd.PersistentFlags().StringVar(&extractorName, "extractor", "", "name of extractor")
	generateExtractorCmd.MarkFlagRequired("extractorName")

	return generateExtractorCmd
}
func Execute() {
	rootCmd.AddCommand(
		runCmd,
		rundirCmd,
		generateExtractorFun(),
	)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
