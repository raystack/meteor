package cmd

import (
	"errors"
	"fmt"
	"github.com/odpf/meteor/plugins"
	"os"

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

			report := [][]string{}
			var success = 0
			var failures = 0

			// Run linters and generate report
			for _, recipe := range recipes {
				errs := runner.Validate(recipe)
				var row []string
				if len(errs) > 0 {
					for _, err := range errs {
						lg.Error(err.Error())
						//getLine(lg, recipe)

						var plug plugins.NotFoundError
						if errors.As(err, &plug) {
							if plug.Type == "extractor" {
								lg.Error("source missing", "source", plug.Name, "line", recipe.Source.Node.Name.Line, "err", err.Error())
							} else if plug.Type == "processor" {
								lg.Error("processor missing", "processor", plug.Name, "line", "--------", "err", err.Error())
							} else if plug.Type == "sink" {
								lg.Error("sink missing", "sink", plug.Name, "line", "??????", "err", err.Error())

							}
							err = nil
						}

						//for _, s := range recipe.Sinks {
						//	sink, err := registry.Sinks.Get(s.Name)
						//	if err != nil {
						//		lg.Error("invalid sink", "sink", s.Name, "line", s.Node.Name.Line)
						//	}
						//}

						var configTest plugins.InvalidConfigError
						if errors.As(err, &configTest) {
							//for i, j := range recipe.Source.Node.Config {
							//	if i == configTest.Key {
							//		lg.Info("line", j.Line)
							//	}x
							//}
							fmt.Println("testing")
							lg.Error("config missing", "line", recipe.Source.Node.Config[configTest.Key].Line, "key", configTest.Key, "err", err.Error())
							err = nil
						}

						//for i, _ := range recipe.Source.Config {
						//
						//	fmt.Println("hy 1")
						//	if errors.As(err, &plugTest) {
						//		lg.Error("config source missing", "source", plugTest.Key, "line", recipe.Source.Node.Config[i].Line, "err", err.Error())
						//		err = nil
						//	}

						//for i, _ := range recipe.Source.Config {
						//	if errors.As(err, &plugins.InvalidConfigError{
						//		Type:       plugins.PluginTypeExtractor,
						//		PluginName: recipe.Source.Name,
						//		Key:        "Config" + i,
						//	}) {
						//		fmt.Println("hy 1")
						//		lg.Error("config source missing", "source", recipe.Source.Name, "line", recipe.Source.Node.Config[i].Line, "err", err.Error())
						//		err = nil
						//	}
						//}

					}
					row = []string{fmt.Sprintf("%s  %s", cs.FailureIcon(), recipe.Name), cs.Greyf("(%d errors, 0 warnings)", len(errs))}
					failures++
				} else {
					row = []string{fmt.Sprintf("%s  %s", cs.SuccessIcon(), recipe.Name), cs.Greyf("(%d errors, 0 warnings)", len(errs))}
					success++
				}
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

func getLine(lg log.Logger, recipe recipe.Recipe) {

	_, err := registry.Extractors.Get(recipe.Source.Name)
	if err != nil {
		lg.Error("invalid source", "source", recipe.Source.Name, "line", recipe.Source.Node.Name.Line)
	}

	for _, p := range recipe.Processors {
		_, err := registry.Sinks.Get(p.Name)
		if err != nil {
			lg.Error("invalid processor", "processor", p.Name, "line", p.Node.Name.Line)
		}
	}

	for _, s := range recipe.Sinks {
		_, err := registry.Sinks.Get(s.Name)
		if err != nil {
			lg.Error("invalid sink", "sink", s.Name, "line", s.Node.Name.Line)
		}
	}
}
