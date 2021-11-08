package cmd

import (
	"fmt"

	"github.com/AlecAivazis/survey/v2"
	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
	"github.com/odpf/salt/printer"
	"github.com/odpf/salt/term"
	"github.com/spf13/cobra"
)

var validArgs = []string{"extractor", "sink", "processor"}

// InfoCmd creates a command object for get info about a plugin
func InfoCmd(lg log.Logger) *cobra.Command {
	validArgs = append(validArgs, listKeys(registry.Sinks.List())...)
	validArgs = append(validArgs, listKeys(registry.Processors.List())...)
	validArgs = append(validArgs, listKeys(registry.Extractors.List())...)

	cmd := &cobra.Command{
		Use:   "info",
		Short: "Display plugin information",
		Long: heredoc.Doc(`
			Display information for various plugins sink, extractor or processor.
			
			The list of supported plugins is available via the 'meteor list <plugin_type>' command.`),
		Example: heredoc.Doc(`
			$ meteor info sink console
			$ meteor info extractor postgres
			$ meteor info processor enrich
		`),
		Annotations: map[string]string{
			"group:core": "true",
		},
		ValidArgs: validArgs,
		Args:      matchAll(cobra.MaximumNArgs(2), cobra.OnlyValidArgs),
		RunE: func(cmd *cobra.Command, args []string) error {
			var plugType string
			if len(args) > 0 {
				plugType = args[0]
			} else {
				if err := survey.AskOne(&survey.Select{
					Message: "Select the type of plugin",
					Options: []string{"extractor", "sink", "processor"},
				}, &plugType); err != nil {
					return err
				}
				args = append(args, plugType)
			}
			switch plugType {
			case "sink":
				return sinkInfo(args)
			case "extractor":
				return extInfo(args)
			case "processor":
				return proccInfo(args)
			}
			return nil
		},
	}
	return cmd
}

// listing sinks
func sinkInfo(args []string) error {
	sinks := listKeys(registry.Sinks.List())
	name, err := getPluginName(args, sinks)

	if err != nil {
		return err
	}

	info, err := registry.Sinks.Info(name)

	if err := inform("sinks", info.Summary, err); err != nil {
		return err
	}
	return nil
}

// listing extractors
func extInfo(args []string) error {
	extrs := listKeys(registry.Extractors.List())
	name, err := getPluginName(args, extrs)

	if err != nil {
		return err
	}

	info, err := registry.Extractors.Info(name)

	if err := inform("extractors", info.Summary, err); err != nil {
		return err
	}
	return nil
}

// listing processors
func proccInfo(args []string) error {
	procs := listKeys(registry.Processors.List())
	name, err := getPluginName(args, procs)

	if err != nil {
		return err
	}

	info, err := registry.Processors.Info(name)

	if err := inform("processors", info.Summary, err); err != nil {
		return err
	}
	return nil
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

func getPluginName(args, plugins []string) (string, error) {
	var name string

	if len(args) > 1 {
		name = args[1]
	} else {
		if err := survey.AskOne(&survey.Select{
			Message: "Select the name of the " + args[0],
			Options: plugins,
		}, &name); err != nil {
			return name, err
		}
	}
	return name, nil
}

func listKeys(pluginMap map[string]plugins.Info) []string {
	var pluginList []string

	for key := range pluginMap {
		pluginList = append(pluginList, key)
	}
	return pluginList
}

func matchAll(checks ...cobra.PositionalArgs) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		for _, check := range checks {
			if err := check(cmd, args); err != nil {
				return err
			}
		}
		return nil
	}
}
