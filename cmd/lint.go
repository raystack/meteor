package cmd

import (
	"errors"
	"fmt"
	"os"

	"github.com/MakeNowJust/heredoc"
	"github.com/goto/meteor/agent"
	"github.com/goto/meteor/config"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/recipe"
	"github.com/goto/meteor/registry"
	"github.com/goto/salt/log"
	"github.com/goto/salt/printer"
	"github.com/goto/salt/term"
	"github.com/spf13/cobra"
)

// LintCmd creates a command object for linting recipes
func LintCmd() *cobra.Command {
	var (
		report   [][]string
		success  = 0
		failures = 0
	)

	return &cobra.Command{
		Use:     "lint [path]",
		Aliases: []string{"l"},
		Args:    cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
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
			cfg, err := config.Load("./meteor.yaml")
			if err != nil {
				return err
			}

			lg := log.NewLogrus(log.LogrusWithLevel(cfg.LogLevel))
			plugins.SetLog(lg)

			runner := agent.NewAgent(agent.Config{
				ExtractorFactory: registry.Extractors,
				ProcessorFactory: registry.Processors,
				SinkFactory:      registry.Sinks,
				Logger:           lg,
			})

			recipes, err := recipe.NewReader(lg, "").Read(args[0])
			if err != nil {
				return err
			}

			if len(recipes) == 0 {
				fmt.Println(term.Yellowf("No recipe found in [%s]", args[0]))
				fmt.Println(term.Blue("\nUse 'meteor gen recipe' to generate a new recipe."))
				return nil
			}

			// Run linters and generate report
			for _, recipe := range recipes {
				errs := runner.Validate(recipe)
				var row []string
				var icon string

				if len(errs) == 0 {
					icon = term.SuccessIcon()
					success++
				} else {
					icon = term.FailureIcon()
					printLintErrors(errs, recipe)
					failures++
				}

				row = []string{fmt.Sprintf("%s  %s", icon, recipe.Name), term.Greyf("(%d errors, 0 warnings)", len(errs))}
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
		notFoundErr   plugins.NotFoundError
		invalidCfgErr plugins.InvalidConfigError
	)

	for _, err := range errs {
		if errors.As(err, &notFoundErr) {
			printPluginErrors(rcp, notFoundErr)
			continue
		}
		if errors.As(err, &invalidCfgErr) {
			printConfigErrors(rcp, invalidCfgErr)
			continue
		}
		fmt.Printf("%s: recipe error: %s\n", rcp.Name, err.Error())
	}
}

// printPluginErrors print the plugin's type error
func printPluginErrors(rcp recipe.Recipe, err plugins.NotFoundError) {
	switch err.Type {
	case plugins.PluginTypeExtractor:
		printPluginError(rcp, rcp.Source, err)

	case plugins.PluginTypeProcessor:
		plugin, exists := findPluginByName(rcp.Processors, err.Name)
		if exists {
			printPluginError(rcp, plugin, err)
		}

	case plugins.PluginTypeSink:
		plugin, exists := findPluginByName(rcp.Sinks, err.Name)
		if exists {
			printPluginError(rcp, plugin, err)
		}
	}
}

// printPluginError prints the plugin type error
func printPluginError(rcp recipe.Recipe, plugin recipe.PluginRecipe, err plugins.NotFoundError) {
	line := plugin.Node.Name.Line
	fmt.Printf("%s: invalid '%s' %s on line: %d\n", rcp.Name, err.Name, err.Type, line)
}

// printConfigErrors print the plugin's config error
func printConfigErrors(rcp recipe.Recipe, err plugins.InvalidConfigError) {
	switch err.Type {
	case plugins.PluginTypeExtractor:
		printConfigError(rcp, rcp.Source.Node, err)

	case plugins.PluginTypeProcessor:
		plugin, exists := findPluginByName(rcp.Processors, err.PluginName)
		if exists {
			printConfigError(rcp, plugin.Node, err)
		}

	case plugins.PluginTypeSink:
		plugin, exists := findPluginByName(rcp.Sinks, err.PluginName)
		if exists {
			printConfigError(rcp, plugin.Node, err)
		}
	}
}

// printConfigError prints the config error in plugin by searching key
func printConfigError(rcp recipe.Recipe, pluginNode recipe.PluginNode, err plugins.InvalidConfigError) {
	for _, cfgErr := range err.Errors {
		cfg, ok := pluginNode.Config[cfgErr.Key]
		if ok {
			line := cfg.Line
			fmt.Printf(
				"%s: invalid %s %s config on line: %d: %s\n",
				rcp.Name, err.PluginName, err.Type, line, cfgErr.Message,
			)
		} else {
			fmt.Printf(
				"%s: invalid %s %s config: %s\n",
				rcp.Name, err.PluginName, err.Type, cfgErr.Message,
			)
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
