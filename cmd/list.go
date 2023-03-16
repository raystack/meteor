package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/MakeNowJust/heredoc"
	"github.com/goto/meteor/registry"
	"github.com/goto/salt/printer"
	"github.com/goto/salt/term"

	"github.com/spf13/cobra"
)

// ListCmd creates a command object for linting recipes
func ListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list <command>",
		Short: "List available plugins",
		Annotations: map[string]string{
			"group:core": "true",
		},
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
		Aliases: []string{"e"},
		Short:   "List available extractors",
		Long: heredoc.Doc(`
			List available extractors.

			This command lists all available extractors.
			Extractors are used to extract metadata from a source.
			For example, you can use an extractor to extract metadata from a file.
		`),
		Example: heredoc.Doc(`
			$ meteor list extractors

			# list all extractors with alias 'e'
			$ meteor list e
		`),
		Annotations: map[string]string{
			"group:core": "true",
		},
		Run: func(cmd *cobra.Command, args []string) {
			extractors := registry.Extractors.List()
			fmt.Printf(" \nShowing %d of %d extractors\n \n", len(extractors), len(extractors))

			report := [][]string{}
			index := 1

			for n, i := range extractors {
				report = append(report, []string{
					term.Greenf("#%02d", index), n, i.Description, term.Greyf(" (%s)", strings.Join(i.Tags, ", ")),
				})
				index++
			}
			printer.Table(os.Stdout, report)
		},
	}
	return cmd
}

// ListSinksCmd creates a command object for listing sinks
func ListSinksCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "sinks",
		Aliases: []string{"s"},
		Short:   "List available sinks",
		Long: heredoc.Doc(`
			List available sinks.

			This command lists all available sinks.
			Sinks are used to send data to a target.
			For example, you can use a sink to send metadata to standard output.`),
		Example: heredoc.Doc(`
			$ meteor list sinks

			# list all sinks with alias 's'
			$ meteor list s
		`),
		Annotations: map[string]string{
			"group:core": "true",
		},
		Run: func(cmd *cobra.Command, args []string) {
			sinks := registry.Sinks.List()
			fmt.Printf(" \nShowing %d of %d sinks\n \n", len(sinks), len(sinks))

			report := [][]string{}
			index := 1
			for n, i := range sinks {
				report = append(report, []string{
					term.Greenf("#%02d", index), n, i.Description, term.Greyf(" (%s)", strings.Join(i.Tags, ", ")),
				})
				index++
			}
			printer.Table(os.Stdout, report)
		},
	}
	return cmd
}

// ListProccCmd creates a command object for listing processors
func ListProccCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "processors",
		Aliases: []string{"p"},
		Short:   "List available processors",
		Long: heredoc.Doc(`
			List available processors.

			This command lists all available processors.
			Processors are used to transform data before it is sent to a sink.
			For example, you can use a processor to enrich custom attributes.`),
		Example: heredoc.Doc(`
			$ meteor list processors

			# list all processors with alias 'p'
			$ meteor list p
		`),
		Annotations: map[string]string{
			"group:core": "true",
		},
		Run: func(cmd *cobra.Command, args []string) {
			processors := registry.Processors.List()
			fmt.Printf(" \nShowing %d of %d processors\n \n", len(processors), len(processors))

			report := [][]string{}
			index := 1

			for n, i := range processors {
				report = append(report, []string{
					term.Greenf("#%02d", index), n, i.Description, term.Greyf(" (%s)", strings.Join(i.Tags, ", ")),
				})
				index++
			}
			printer.Table(os.Stdout, report)
		},
	}
	return cmd
}
