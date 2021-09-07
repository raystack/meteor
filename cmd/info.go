package cmd

import (
	"fmt"

	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
	"github.com/odpf/salt/printer"
	"github.com/odpf/salt/term"
	"github.com/spf13/cobra"
)

// InfoCmd creates a command object for get info about a plugin
func InfoCmd(lg log.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "info <command>",
		Short: "View plugin information",
		Annotations: map[string]string{
			"group:core": "true",
		},
	}
	cmd.AddCommand(InfoSinkCmd())
	cmd.AddCommand(InfoExtCmd())
	cmd.AddCommand(InfoProccCmd())
	return cmd
}

// InfoSinkCmd creates a command object for listing sinks
func InfoSinkCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sink <name>",
		Short: "Vew sink information",
		Long: heredoc.Doc(`
			View sink information.
			
			The list of supported sinks is available via the 'meteor list sinks' command.
		`),
		Example: heredoc.Doc(`
			$ meteor info sink console
		`),
		Args: cobra.ExactArgs(1),
		Annotations: map[string]string{
			"group:core": "true",
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			info, err := registry.Sinks.Info(name)

			if err := inform("sinks", info.Summary, err); err != nil {
				return err
			}
			return nil
		},
	}
	return cmd
}

// InfoExtCmd creates a command object for listing extractors
func InfoExtCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "extractor <name>",
		Short: "Vew extractor information",
		Long: heredoc.Doc(`
			View sink information.
			
			The list of supported extractors is available via the 'meteor list extractors' command.
		`),
		Example: heredoc.Doc(`
			$ meteor info sink console
		`),
		Args: cobra.ExactArgs(1),
		Annotations: map[string]string{
			"group:core": "true",
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			info, err := registry.Extractors.Info(name)
			if err := inform("extractors", info.Summary, err); err != nil {
				return err
			}
			return nil
		},
	}
	return cmd
}

// InfoProccCmd creates a command object for listing processors
func InfoProccCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "processor <name>",
		Short: "Vew processor information",
		Long: heredoc.Doc(`
			View processor information.
			
			The list of supported processors is available via the 'meteor list processors' command.
		`),
		Example: heredoc.Doc(`
			$ meteor info sink console
		`),
		Args: cobra.ExactArgs(1),
		Annotations: map[string]string{
			"group:core": "true",
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			info, err := registry.Processors.Info(name)

			if err := inform("processors", info.Summary, err); err != nil {
				return err
			}
			return nil
		},
	}
	return cmd
}

func inform(typ string, summary string, err error) error {
	cs := term.NewColorScheme()

	if err != nil {
		fmt.Println(cs.Redf("ERROR:"), cs.Redf(err.Error()))
		fmt.Println(cs.Bluef("\nUse 'meteor list %s' for the list of supported %s.", typ, typ))
		return nil
	}

	out, err := printer.MarkdownWithWrap(summary, 130)

	if err != nil {
		return err
	}
	fmt.Print(out)
	return nil
}
