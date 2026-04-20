package cmd

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/MakeNowJust/heredoc"
	"github.com/raystack/meteor/recipe"
	"github.com/raystack/meteor/registry"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

// RecipeCmd creates the top-level recipe command.
func RecipeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "recipe <command>",
		Short: "Manage recipes",
		Annotations: map[string]string{
			"group": "core",
		},
	}
	cmd.AddCommand(recipeInitCmd())
	cmd.AddCommand(recipeGenCmd())
	return cmd
}

func recipeInitCmd() *cobra.Command {
	var (
		extractor  string
		scope      string
		sinks      string
		processors string
	)

	cmd := &cobra.Command{
		Use:   "init [name]",
		Args:  cobra.ExactArgs(1),
		Short: "Bootstrap a new recipe",
		Long: heredoc.Doc(`
			Bootstrap a new recipe.

			The recipe will be printed on standard output.
			Specify recipe name with the first argument without extension.
			Use comma to separate multiple sinks and processors.`),
		Example: heredoc.Doc(`
			# bootstrap a recipe with a bigquery extractor and a console sink
			$ meteor recipe init sample -e bigquery -s console

			# bootstrap recipe with multiple sinks
			$ meteor recipe init sample -e bigquery -s compass,kafka -p enrich

			# store recipe to a file
			$ meteor recipe init sample -e bigquery -s compass > recipe.yaml
		`),
		Annotations: map[string]string{
			"group": "core",
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			var sinkList []string
			var procList []string
			var err error

			if extractor == "" {
				extractor, err = recipeExtractorSurvey()
				if err != nil {
					return err
				}
			}

			if sinks != "" {
				sinkList = strings.Split(sinks, ",")
			} else {
				sinkList, err = recipeSinkSurvey()
				if err != nil {
					return err
				}
			}

			if processors != "" {
				procList = strings.Split(processors, ",")
			} else {
				procList, err = recipeProcessorSurvey()
				if err != nil {
					return err
				}
			}

			return recipe.ScaffoldWriteTo(recipe.ScaffoldParams{
				Name:       args[0],
				Source:     extractor,
				Scope:      scope,
				Sinks:      sinkList,
				Processors: procList,
			}, os.Stdout)
		},
	}

	cmd.Flags().StringVarP(&extractor, "extractor", "e", "", "Type of extractor")
	cmd.Flags().StringVarP(&scope, "scope", "n", "", "URN's namespace")
	cmd.Flags().StringVarP(&sinks, "sinks", "s", "", "Comma-separated list of sink types")
	cmd.Flags().StringVarP(&processors, "processors", "p", "", "Comma-separated list of processor types")

	if err := cmd.MarkFlagRequired("scope"); err != nil {
		panic(err)
	}

	return cmd
}

func recipeGenCmd() *cobra.Command {
	var (
		outputDirPath string
		dataFilePath  string
	)

	cmd := &cobra.Command{
		Use:   "gen <template-path>",
		Args:  cobra.ExactArgs(1),
		Short: "Generate recipes from a template",
		Long: heredoc.Doc(`
			Generate multiple recipes using a template and list of data.

			The generated recipes will be created in the output directory.`,
		),
		Example: heredoc.Doc(`
			$ meteor recipe gen my-template.yaml -o ./output-dir -d ./data.yaml
		`),
		Annotations: map[string]string{
			"group": "core",
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			templatePath := args[0]

			bytes, err := os.ReadFile(dataFilePath)
			if err != nil {
				return fmt.Errorf("error reading data: %w", err)
			}
			var data []recipe.TemplateData
			if err := yaml.Unmarshal(bytes, &data); err != nil {
				return fmt.Errorf("error parsing data: %w", err)
			}

			return recipe.FromTemplate(recipe.TemplateConfig{
				TemplateFilePath: templatePath,
				OutputDirPath:    outputDirPath,
				Data:             data,
			})
		},
	}

	cmd.Flags().StringVarP(&outputDirPath, "output", "o", "", "Output directory")
	cmd.Flags().StringVarP(&dataFilePath, "data", "d", "", "Template's data file")

	return cmd
}

func recipeSinkSurvey() ([]string, error) {
	var availableSinks []string
	var sinkInput []string

	for sink := range registry.Sinks.List() {
		availableSinks = append(availableSinks, sink)
	}
	if len(availableSinks) == 0 {
		return []string{}, errors.New("no sinks found")
	}

	var qs = []*survey.Question{
		{
			Name: "sink",
			Prompt: &survey.MultiSelect{
				Message: "Select sink(s)",
				Options: availableSinks,
				Help:    "Select the sink(s) for this recipe",
			},
			Validate: survey.Required,
		},
	}

	if err := survey.Ask(qs, &sinkInput); err != nil {
		return []string{}, err
	}

	return sinkInput, nil
}

func recipeProcessorSurvey() ([]string, error) {
	var availableProcessors []string
	var processorInput []string

	for processor := range registry.Processors.List() {
		availableProcessors = append(availableProcessors, processor)
	}
	if len(availableProcessors) == 0 {
		return []string{}, errors.New("no processors found")
	}

	var qs = []*survey.Question{
		{
			Name: "processor",
			Prompt: &survey.MultiSelect{
				Message: "Select processor(s)",
				Options: availableProcessors,
				Help:    "Select the processor(s) for this recipe",
			},
		},
	}

	if err := survey.Ask(qs, &processorInput); err != nil {
		return []string{}, err
	}

	return processorInput, nil
}

func recipeExtractorSurvey() (string, error) {
	var availableExtractors []string
	var extractorInput string

	for extractor := range registry.Extractors.List() {
		availableExtractors = append(availableExtractors, extractor)
	}
	if len(availableExtractors) == 0 {
		return "", errors.New("no extractors found")
	}

	var qs = []*survey.Question{
		{
			Name: "extractor",
			Prompt: &survey.Select{
				Message: "Select an extractor",
				Options: availableExtractors,
				Help:    "Select the extractor for this recipe",
			},
			Validate: survey.Required,
		},
	}

	if err := survey.Ask(qs, &extractorInput); err != nil {
		return "", err
	}

	return extractorInput, nil
}
