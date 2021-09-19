package cmd

import (
	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/meteor/metrics"
	"github.com/odpf/salt/cmdx"
	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
)

// New adds all child commands to the root command and sets flags appropriately.
func New(lg log.Logger, mt *metrics.StatsdMonitor) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "meteor <command> <subcommand> [flags]",
		Short: "Metadata CLI",
		Long: heredoc.Doc(`
			Metadata collection tool.

			Meteor is a plugin driven agent for collecting metadata. 
			Meteor has plugins to source metadata from a variety of data stores, 
			services and message queues. It also has sink plugins to send metadata 
			to variety of third party APIs and catalog services.`),
		SilenceErrors: true,
		SilenceUsage:  false,
		Example: heredoc.Doc(`
			$ meteor list extractors
			$ meteor run recipe.yaml
			$ meteor gen recipe --extractor=date --sink console
		`),
		Annotations: map[string]string{
			"group:core": "true",
			"help:feedback": heredoc.Doc(`
				Open an issue here https://github.com/odpf/meteor/issues
			`),
		},
	}

	cmdx.SetHelp(cmd)

	cmd.AddCommand(VersionCmd())
	cmd.AddCommand(GenCmd(lg))
	cmd.AddCommand(ListCmd(lg))
	cmd.AddCommand(InfoCmd(lg))
	cmd.AddCommand(RunCmd(lg, mt))
	cmd.AddCommand(LintCmd(lg, mt))

	return cmd
}
