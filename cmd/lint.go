package cmd

import (
	"errors"
	"fmt"
	"os"

	"github.com/odpf/meteor/plugins"

	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/meteor/agent"
	"github.com/odpf/meteor/metrics"
	"github.com/odpf/meteor/recipe"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
	"github.com/odpf/salt/printer"
	"github.com/odpf/salt/term"

	"github.com/spf13/cobra"
)

// LintCmd creates a command object for linting recipes
func LintCmd(lg log.Logger, mt *metrics.StatsdMonitor) *cobra.Command {
	var (
		report   [][]string
		success  = 0
		failures = 0
	)

	return &cobra.Command{
		Use:     "lint [path]",
		Aliases: []string{"l"},
		Args:    cobra.ExactValidArgs(1),
		Short:   "Check for issues in recipes",
		Long: heredoc.Doc(`
			Check for issues specified recipes.

			Linters are run on the recipe files in the specified path.
			If no path is specified, the current directory is used.`),
		Example: heredoc.Doc(`
			$ meteor lint recipe.yml

			# lint all recipes in the specified directory
			$ meteor lint _recipes/

			# lint all recipes in the current directory
			$ meteor lint .
		`),
		Annotations: map[string]string{
			"group:core": "true",
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			cs := term.NewColorScheme()
			runner := agent.NewAgent(agent.Config{
				ExtractorFactory: registry.Extractors,
				ProcessorFactory: registry.Processors,
				SinkFactory:      registry.Sinks,
				Monitor:          mt,
				Logger:           lg,
			})

			recipes, err := recipe.NewReader().Read(args[0])
			if err != nil {
				return err
			}

			if len(recipes) == 0 {
				fmt.Println(cs.Yellowf("No recipe found in [%s]", args[0]))
				fmt.Println(cs.Blue("\nUse 'meteor gen recipe' to generate a new recipe."))
				return nil
			}

			// Run linters and generate report
			for _, recipe := range recipes {
				errs := runner.Validate(recipe)
				var row []string
				var icon string

				if len(errs) == 0 {
					icon = cs.SuccessIcon()
					success++
				} else {
					icon = cs.FailureIcon()
					printLintErrors(errs, recipe)
					failures++
				}

				row = []string{fmt.Sprintf("%s  %s", icon, recipe.Name), cs.Greyf("(%d errors, 0 warnings)", len(errs))}
				report = append(report, row)
			}

			// Print the report
			if failures > 0 {
				fmt.Println("\nSome checks were not successful")
			} else {
				fmt.Println("\nAll checks were successful")
			}
			fmt.Printf("%d failing, %d successful, and %d total\n\n", failures, success, len(recipes))
			printer.Table(os.Stdout, report)

			return nil
		},
	}
}

// printLintErrors prints the recipe errors
func printLintErrors(errs []error, rcp recipe.Recipe) {
	var (
		notFoundError      plugins.NotFoundError
		invalidConfigError plugins.InvalidConfigError
	)

	for _, err := range errs {
		if errors.As(err, &notFoundError) {
			printPluginErrors(rcp, notFoundError)
			continue
		}
		if errors.As(err, &invalidConfigError) {
			printConfigErrors(rcp, invalidConfigError)
			continue
		}
		fmt.Printf("recipe error: %s", err.Error())
	}
}

// printPluginErrors print the plugin's type error
func printPluginErrors(rcp recipe.Recipe, notFoundError plugins.NotFoundError) {
	if notFoundError.Type == plugins.PluginTypeExtractor {
		printPluginError(rcp, rcp.Source, notFoundError)
	} else if notFoundError.Type == plugins.PluginTypeProcessor {
		plugin, exists := findPluginByName(rcp.Processors, notFoundError.Name)
		if exists {
			printPluginError(rcp, plugin, notFoundError)
		}
	} else if notFoundError.Type == plugins.PluginTypeSink {
		plugin, exists := findPluginByName(rcp.Sinks, notFoundError.Name)
		if exists {
			printPluginError(rcp, plugin, notFoundError)
		}
	}
}

// printPluginError prints the plugin type error
func printPluginError(rcp recipe.Recipe, plugin recipe.PluginRecipe, notFoundError plugins.NotFoundError) {
	line := plugin.Node.Name.Line
	fmt.Printf("%s: invalid %s on line: %d\n", rcp.Name, notFoundError.Type, line)
}

// printConfigErrors print the plugin's config error
func printConfigErrors(rcp recipe.Recipe, invalidConfigError plugins.InvalidConfigError) {
	if invalidConfigError.Type == plugins.PluginTypeExtractor {
		printConfigError(rcp, rcp.Source.Node, invalidConfigError)
	} else if invalidConfigError.Type == plugins.PluginTypeProcessor {
		plugin, exists := findPluginByName(rcp.Processors, invalidConfigError.PluginName)
		if exists {
			printConfigError(rcp, plugin.Node, invalidConfigError)
		}
	} else if invalidConfigError.Type == plugins.PluginTypeSink {
		plugin, exists := findPluginByName(rcp.Sinks, invalidConfigError.PluginName)
		if exists {
			printConfigError(rcp, plugin.Node, invalidConfigError)
		}
	}
}

// printConfigError prints the config error in plugin by searching key
func printConfigError(rcp recipe.Recipe, pluginNode recipe.PluginNode, invalidConfigError plugins.InvalidConfigError) {
	for _, configError := range invalidConfigError.Errors {
		cfg, ok := pluginNode.Config[configError.Key]
		if ok {
			line := cfg.Line
			fmt.Printf("%s: invalid %s %s config on line: %d\n", rcp.Name, invalidConfigError.PluginName, invalidConfigError.Type, line)
		} else {
			fmt.Printf("%s: invalid %s %s config: %s\n", rcp.Name, invalidConfigError.PluginName, invalidConfigError.Type, configError.Message)
		}
	}
}

// findPluginByName checks plugin by provided name
func findPluginByName(plugins []recipe.PluginRecipe, name string) (plugin recipe.PluginRecipe, exists bool) {
	for _, p := range plugins {
		if p.Name == name {
			exists = true
			plugin = p
			return
		}
	}

	return
}
