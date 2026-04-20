package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/raystack/meteor/runner"
	"github.com/raystack/meteor/config"
	"github.com/raystack/meteor/metrics"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/recipe"
	"github.com/raystack/meteor/registry"
	"github.com/raystack/salt/cli/printer"
	log "github.com/raystack/salt/observability/logger"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
)

// RunCmd creates a command object for the "run" action.
func RunCmd() *cobra.Command {
	var (
		report       [][]string
		pathToConfig string
		success      = 0
		failures     = 0
		configFile   string
		logLevel     string
		dryRun       bool
		recordLimit  int
	)

	cmd := &cobra.Command{
		Use:   "run <path>|<name>",
		Short: "Execute recipes for metadata extraction",
		Long: heredoc.Doc(`
			Execute specified recipes for metadata extraction.

			A recipe is a set of instructions and configurations defined by user,
			and in Meteor they are used to define how metadata will be collected.

			If a recipe file is provided, recipe will be executed as a single recipe.
			If a recipe directory is provided, recipes will be executed as a group of recipes.`),
		Example: heredoc.Doc(`
			$ meteor run recipe.yml

			# run all recipes in the specified directory
			$ meteor run _recipes/

			# run all recipes in the current directory
			$ meteor run .

			# dry-run to preview extracted records without sending to sinks
			$ meteor run recipe.yml --dry-run

			# extract only the first 10 records for testing
			$ meteor run recipe.yml --dry-run --limit 10
		`),
		Args: cobra.ExactArgs(1),
		Annotations: map[string]string{
			"group": "core",
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(configFile)
			if err != nil {
				return err
			}

			if logLevel != "" {
				cfg.LogLevel = logLevel
			}

			lg := log.NewLogrus(log.LogrusWithLevel(cfg.LogLevel))
			plugins.SetLog(lg)

			// Monitoring system signals and creating context
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			var mts runner.Monitor

			if cfg.OtelEnabled {
				doneOtlp, err := metrics.InitOtel(ctx, cfg, lg, Version)
				if err != nil {
					return err
				}
				defer doneOtlp()

				mts = metrics.NewOtelMonitor()
			}

			rnr := runner.NewRunner(runner.Config{
				ExtractorFactory:     registry.Extractors,
				ProcessorFactory:     registry.Processors,
				SinkFactory:          registry.Sinks,
				Monitor:              mts,
				Logger:               lg,
				MaxRetries:           cfg.MaxRetries,
				RetryInitialInterval: time.Duration(cfg.RetryInitialIntervalSeconds) * time.Second,
				StopOnSinkError:      cfg.StopOnSinkError,
				SinkBatchSize:        cfg.SinkBatchSize,
				DryRun:               dryRun,
				RecordLimit:          recordLimit,
			})

			recipes, err := recipe.NewReader(lg, pathToConfig).Read(args[0])
			if err != nil {
				return err
			}

			if len(recipes) == 0 {
				fmt.Println(printer.Icon("warning"), printer.Yellowf("No recipe found in [%s]", args[0]))
				return nil
			}

			if dryRun {
				fmt.Println(printer.Yellowf("Dry-run mode: sinks will be skipped"))
				if recordLimit > 0 {
					fmt.Printf("Record limit: %d\n", recordLimit)
				}
				fmt.Println()
			}

			report = append(report, []string{"Status", "Recipe", "Source", "Duration(ms)", "Records", "Entity Types"})

			bar := progressbar.NewOptions(len(recipes),
				progressbar.OptionEnableColorCodes(true),
				progressbar.OptionSetDescription("[cyan]running recipes [reset]"),
				progressbar.OptionShowCount(),
			)

			// Run recipes and collect results
			runs := rnr.RunMultiple(ctx, recipes)
			for _, run := range runs {
				lg.Debug("recipe details", "recipe", run.Recipe)
				var row []string
				entitySummary := formatEntityTypes(run.EntityTypes)
				if run.Error != nil {
					lg.Error(run.Error.Error(), "recipe", run.Recipe.Name)
					failures++
					row = append(row, printer.Icon("failure"), run.Recipe.Name, printer.Grey(run.Recipe.Source.Name), printer.Greyf("%v ms", strconv.Itoa(run.DurationInMs)), printer.Greyf("%s", strconv.Itoa(run.RecordCount)), printer.Greyf("%s", entitySummary))
				} else {
					success++
					row = append(row, printer.Icon("success"), run.Recipe.Name, printer.Grey(run.Recipe.Source.Name), printer.Greyf("%v ms", strconv.Itoa(run.DurationInMs)), printer.Greyf("%s", strconv.Itoa(run.RecordCount)), printer.Greyf("%s", entitySummary))
				}
				report = append(report, row)
				if err = bar.Add(1); err != nil {
					return err
				}
			}

			// Print the report
			if failures > 0 {
				fmt.Println("\nSome recipes were not successful")
				printRunErrors(runs)
				err = fmt.Errorf("%d recipes failed", failures)
				// Disable usage message on recipe failure
				cmd.SilenceUsage = true
			} else {
				fmt.Println("\nAll recipes ran successful")
			}
			fmt.Printf("%d failing, %d successful, and %d total\n\n", failures, success, len(recipes))
			printer.Table(os.Stdout, report)
			return err
		},
	}

	cmd.Flags().StringVar(&pathToConfig, "var", "", "Path to Config file with env variables for recipe")
	cmd.Flags().StringVarP(&configFile, "config", "c", "./meteor.yaml", "file path for agent level config")
	cmd.Flags().StringVar(&logLevel, "log-level", "", "Override log level (debug, info, warn, error)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Extract records without sending to sinks")
	cmd.Flags().IntVar(&recordLimit, "limit", 0, "Maximum number of records to extract (0 = unlimited)")

	return cmd
}

// formatEntityTypes returns a compact summary of entity types.
func formatEntityTypes(types map[string]int) string {
	if len(types) == 0 {
		return "-"
	}

	keys := make([]string, 0, len(types))
	for k := range types {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s:%d", k, types[k]))
	}
	return strings.Join(parts, ", ")
}

// printRunErrors prints error details for failed runs.
func printRunErrors(runs []runner.Run) {
	for _, run := range runs {
		if run.Error != nil {
			fmt.Printf("  %s %s: %s\n", printer.Icon("failure"), run.Recipe.Name, run.Error)
		}
	}
}
