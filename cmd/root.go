package cmd

import (
	"fmt"
	"os"

	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/meteor/config"
	"github.com/odpf/meteor/metrics"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/salt/cmdx"
	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
)

const exitError = 1

// New adds all child commands to the root command and sets flags appropriately.
func New() *cobra.Command {
	cfg, err := config.Load("./meteor.yaml")
	if err != nil {
		fmt.Printf("ERROR: %s\n", err.Error())
		os.Exit(1)
	}

	lg := log.NewLogrus(log.LogrusWithLevel(cfg.LogLevel))
	plugins.SetLog(lg)

	// Setup statsd monitor to collect monitoring metrics
	var mt *metrics.StatsdMonitor
	if cfg.StatsdEnabled {
		client, err := metrics.NewStatsdClient(cfg.StatsdHost)
		if err != nil {
			fmt.Printf("ERROR: %s\n", err.Error())
			os.Exit(exitError)
		}
		mt = metrics.NewStatsdMonitor(client, cfg.StatsdPrefix)
	}

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
				Read the manual at https://odpf.github.io/meteor/
			`),
			"help:feedback": heredoc.Doc(`
				Open an issue here https://github.com/odpf/meteor/issues
			`),
		},
	}

	// Help topics
	cmdx.SetHelp(cmd)
	cmd.AddCommand(cmdx.SetCompletionCmd("meteor"))
	cmd.AddCommand(cmdx.SetRefCmd(cmd))

	cmd.AddCommand(VersionCmd())
	cmd.AddCommand(GenCmd(lg))
	cmd.AddCommand(ListCmd(lg))
	cmd.AddCommand(InfoCmd(lg))
	cmd.AddCommand(RunCmd(lg, mt, cfg))
	cmd.AddCommand(LintCmd(lg, mt))
	cmd.AddCommand(NewCmd(lg))

	return cmd
}
