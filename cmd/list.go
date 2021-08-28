package cmd

import (
	"fmt"
	"os"

	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
	"github.com/odpf/salt/printer"

	"github.com/spf13/cobra"
)

// LintCmd creates a command object for linting recipes
func ListCmd(lg log.Logger) *cobra.Command {

	cmd := &cobra.Command{
		Use:   "list <command>",
		Short: "List available plugins in meteor",
	}
	cmd.AddCommand(ListExtCmd())
	cmd.AddCommand(ListSinksCmd())
	cmd.AddCommand(ListProccCmd())
	return cmd
}

// ListExtCmd creates a command object for listing extractors
func ListExtCmd() *cobra.Command {

	cmd := &cobra.Command{
		Use:     "extractors",
		Example: "meteor list extractors",
		Short:   "List available extractors",
		Run: func(cmd *cobra.Command, args []string) {
			extractors := registry.Extractors.List()
			fmt.Printf(" \nShowing %d of %d extractors\n \n", len(extractors), len(extractors))
			printer.Table(os.Stdout, extractors)
		},
	}
	return cmd
}

//  ListSinksCmd creates a command object for listing sinks
func ListSinksCmd() *cobra.Command {

	cmd := &cobra.Command{
		Use:     "sinks",
		Example: "meteor list sinks",
		Short:   "List available sinks",
		Run: func(cmd *cobra.Command, args []string) {
			sinks := registry.Sinks.List()
			fmt.Printf(" \nShowing %d of %d sinks\n \n", len(sinks), len(sinks))
			printer.Table(os.Stdout, sinks)
		},
	}
	return cmd
}

//  ListProccCmd creates a command object for listing processors
func ListProccCmd() *cobra.Command {

	cmd := &cobra.Command{
		Use:     "processors",
		Example: "meteor list processors",
		Short:   "List available processors",
		Run: func(cmd *cobra.Command, args []string) {
			processors := registry.Processors.List()
			fmt.Printf(" \nShowing %d of %d processors\n \n", len(processors), len(processors))
			printer.Table(os.Stdout, processors)
		},
	}
	return cmd
}
