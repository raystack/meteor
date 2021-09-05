package cmd

import (
	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/meteor/metrics"
	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
)

// Execute adds all child commands to the root command and sets flags appropriately.
func New(lg log.Logger, mt *metrics.StatsdMonitor) *cobra.Command {
	var cmd = &cobra.Command{
		Use:           "meteor <command> <subcommand> [flags]",
		Short:         "Metadata CLI",
		Long:          heredoc.Doc(`Metadata collection tool.`),
		SilenceErrors: true,
		SilenceUsage:  true,
		Example: heredoc.Doc(`
			$ meteor list extractors
			$ meteor run recipe.yaml
			$ meteor gen recipe --extractor=date
		`),
		Annotations: map[string]string{
			"group:core": "true",
			"help:feedback": heredoc.Doc(`
				Open an issue here https://github.com/odpf/meteor/issues
			`),
		},
	}

	cmd.PersistentFlags().Bool("help", false, "Show help for command")

	cmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		RootHelpFunc(cmd, args)
	})
	cmd.SetUsageFunc(RootUsageFunc)
	cmd.SetFlagErrorFunc(RootFlagErrorFunc)

	cmd.AddCommand(RunCmd(lg, mt))
	cmd.AddCommand(GenCmd(lg))
	cmd.AddCommand(ListCmd(lg))
	cmd.AddCommand(LintCmd(lg))
	cmd.AddCommand(InfoCmd(lg))

	return cmd
}
