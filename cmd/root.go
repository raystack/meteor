package cmd

import (
	"github.com/MakeNowJust/heredoc"
	"github.com/goto/salt/cmdx"
	"github.com/spf13/cobra"
)

// New adds all child commands to the root command and sets flags appropriately.
func New() *cobra.Command {
	var cmd = &cobra.Command{
		Use:           "meteor <command> <subcommand> [flags]",
		Short:         "Metadata CLI",
		Long:          "Metadata collection tool.",
		SilenceErrors: true,
		SilenceUsage:  false,
		Example: heredoc.Doc(`
			$ meteor list extractors
			$ meteor run recipe.yaml
			$ meteor gen recipe --extractor=date --sink console
		`),
		Annotations: map[string]string{
			"group:core": "true",
			"help:learn": heredoc.Doc(`
				Use 'meteor <command> <subcommand> --help' for more information about a command.
				Read the manual at https://goto.github.io/meteor/
			`),
			"help:feedback": heredoc.Doc(`
				Open an issue here https://github.com/goto/meteor/issues
			`),
		},
	}

	// Help topics
	cmdx.SetHelp(cmd)
	cmd.AddCommand(cmdx.SetCompletionCmd("meteor"))
	cmd.AddCommand(cmdx.SetRefCmd(cmd))

	cmd.AddCommand(VersionCmd())
	cmd.AddCommand(GenCmd())
	cmd.AddCommand(ListCmd())
	cmd.AddCommand(InfoCmd())
	cmd.AddCommand(RunCmd())
	cmd.AddCommand(LintCmd())
	cmd.AddCommand(NewCmd())

	return cmd
}
