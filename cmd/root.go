package cmd

import (
	"github.com/odpf/meteor/metrics"
	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
)

// Execute adds all child commands to the root command and sets flags appropriately.
func New(lg log.Logger, mt *metrics.StatsdMonitor) *cobra.Command {

	var cmd = &cobra.Command{
		Use:   "meteor",
		Short: "Metadata collection tool",
		Long:  "Meteor is a plugin driven agent for collecting metadata from a variety of data stores and sink to third party APIs and catalog services.",
	}

	cmd.AddCommand(RunCmd(lg, mt))
	cmd.AddCommand(GenCmd(lg))
	cmd.AddCommand(ListCmd(lg))
	cmd.AddCommand(LintCmd(lg))

	return cmd
}
