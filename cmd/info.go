package cmd

import (
	"fmt"

	"github.com/AlecAivazis/survey/v2"
	"github.com/MakeNowJust/heredoc"
	"github.com/goto/meteor/registry"
	"github.com/goto/salt/printer"
	"github.com/goto/salt/term"
	"github.com/spf13/cobra"
)

// InfoCmd creates a command object for get info about a plugin
func InfoCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "info <command>",
		Short: "Display plugin information",
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
		Short: "Display sink information",
		Long: heredoc.Doc(`
			Display sink information.
			
			The list of supported sinks is available via the 'meteor list sinks' command.`),
		Example: heredoc.Doc(`
			$ meteor info sink console
			$ meteor info sink compass
		`),
		Args: cobra.MaximumNArgs(1),
		Annotations: map[string]string{
			"group:core": "true",
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			var sinks []string
			for n := range registry.Sinks.List() {
				sinks = append(sinks, n)
			}
			// checking if plugin_name was passed as an Arg
			var name string
			if len(args) == 1 {
				name = args[0]
			} else {
				if err := survey.AskOne(&survey.Select{
					Message: "Select the name of sink",
					Options: sinks,
				}, &name); err != nil {
					return err
				}
			}
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
		Short: "Display extractor information",
		Long: heredoc.Doc(`
			Display sink information.
			
			The list of supported extractors is available via the 'meteor list extractors' command.
		`),
		Example: heredoc.Doc(`
			$ meteor info extractor postgres
			$ meteor info extractor bigquery
		`),
		Args: cobra.MaximumNArgs(1),
		Annotations: map[string]string{
			"group:core": "true",
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			var extractors []string
			for n := range registry.Extractors.List() {
				extractors = append(extractors, n)
			}
			// checking if plugin_name was passed as an Arg
			var name string
			if len(args) == 1 {
				name = args[0]
			} else {
				if err := survey.AskOne(&survey.Select{
					Message: "Select the name of extractor",
					Options: extractors,
				}, &name); err != nil {
					return err
				}
			}
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
		Short: "Display processor information",
		Long: heredoc.Doc(`
			Display processor information.
			
			The list of supported processors is available via the 'meteor list processors' command.
		`),
		Example: heredoc.Doc(`
			$ meteor info processor enrich
		`),
		Args: cobra.MaximumNArgs(1),
		Annotations: map[string]string{
			"group:core": "true",
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			var processors []string
			for n := range registry.Processors.List() {
				processors = append(processors, n)
			}
			var name string
			if len(args) > 0 {
				name = args[0]
			} else {
				if err := survey.AskOne(&survey.Select{
					Message: "Select the name of the Processor",
					Options: processors,
				}, &name); err != nil {
					return err
				}
			}
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
	if err != nil {
		fmt.Println(term.Redf("ERROR:"), term.Redf(err.Error()))
		fmt.Println(term.Bluef("\nUse 'meteor list %s' for the list of supported %s.", typ, typ))
		return nil
	}

	out, err := printer.MarkdownWithWrap(summary, 130)

	if err != nil {
		return err
	}
	fmt.Print(out)
	return nil
}
