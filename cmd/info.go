package cmd

import (
	"fmt"

	"github.com/charmbracelet/glamour"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
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
		Use:     "sink <name>",
		Example: "meteor info sink console",
		Short:   "Vew an sink information",
		Args:    cobra.ExactArgs(1),
		Annotations: map[string]string{
			"group:core": "true",
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			info, _ := registry.Sinks.Info(name)

			r, _ := glamour.NewTermRenderer(
				glamour.WithAutoStyle(),
			)

			out, _ := r.Render(info.Summary)
			fmt.Print(out)

			return nil
		},
	}
	return cmd
}

// InfoExtCmd creates a command object for listing extractors
func InfoExtCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "extractor <name>",
		Example: "meteor info extractor kafka",
		Short:   "Vew extractor information",
		Args:    cobra.ExactArgs(1),
		Annotations: map[string]string{
			"group:core": "true",
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			info, _ := registry.Extractors.Info(name)

			r, _ := glamour.NewTermRenderer(
				glamour.WithAutoStyle(),
			)

			out, _ := r.Render(info.Summary)
			fmt.Print(out)

			return nil
		},
	}
	return cmd
}

// InfoProccCmd creates a command object for listing processors
func InfoProccCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "processor <name>",
		Example: "meteor info processor enrich",
		Short:   "Vew processor information",
		Args:    cobra.ExactArgs(1),
		Annotations: map[string]string{
			"group:core": "true",
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			info, _ := registry.Processors.Info(name)

			r, _ := glamour.NewTermRenderer(
				glamour.WithAutoStyle(),
			)

			out, _ := r.Render(info.Summary)
			fmt.Print(out)

			return nil
		},
	}
	return cmd
}
