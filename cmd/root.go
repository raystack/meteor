package cmd

import (
	"github.com/MakeNowJust/heredoc"
	"github.com/raystack/salt/cli/commander"
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
			$ meteor run recipe.yaml
			$ meteor lint recipe.yaml
			$ meteor plugins list
			$ meteor plugins info bigquery
			$ meteor recipe init sample -n mycompany -e bigquery -s compass
		`),
		Annotations: map[string]string{
			"group": "core",
			"help:learn": heredoc.Doc(`
				Use 'meteor <command> <subcommand> --help' for more information about a command.
				Read the manual at https://raystack.github.io/meteor/
			`),
			"help:feedback": heredoc.Doc(`
				Open an issue here https://github.com/raystack/meteor/issues
			`),
		},
	}

	commander.New(cmd).Init()

	cmd.AddCommand(RunCmd())
	cmd.AddCommand(LintCmd())
	cmd.AddCommand(RecipeCmd())
	cmd.AddCommand(PluginsCmd())
	cmd.AddCommand(EntitiesCmd())
	cmd.AddCommand(EdgesCmd())
	cmd.AddCommand(VersionCmd())

	return cmd
}
