package cmd

import (
	"errors"
	"os"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/MakeNowJust/heredoc"
	"github.com/goto/meteor/generator"
	"github.com/goto/meteor/registry"
	"github.com/spf13/cobra"
)

// NewCmd creates a command object for the "new" action
func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "new",
		Short: "Bootstrap new recipes",
		Annotations: map[string]string{
			"group:core": "true",
		},
	}

	cmd.AddCommand(NewRecipeCmd())

	return cmd
}

// NewRecipeCmd creates a command object for newerating recipes
func NewRecipeCmd() *cobra.Command {
	var (
		extractor  string
		scope      string
		sinks      string
		processors string
	)

	cmd := &cobra.Command{
		Use:     "recipe [name]",
		Aliases: []string{"r"},
		Args:    cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		Short:   "Generate a new recipe",
		Long: heredoc.Doc(`
			Generate a new recipe.

			The recipe will be printed on standard output.
			Specify recipe name with the first argument without extension.
			Use commma to separate multiple sinks and processors.`),
		Example: heredoc.Doc(`
			# generate a recipe with a bigquery extractor and a console sink
			$ meteor new recipe sample -e bigquery -s console

			# generate recipe with multiple sinks
			$ meteor new recipe sample -e bigquery -s compass,kafka -p enrich

			# store recipe to a file
			$ meteor new recipe sample -e bigquery -s compass > recipe.yaml
		`),
		Annotations: map[string]string{
			"group:core": "true",
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

			return generator.RecipeWriteTo(generator.RecipeParams{
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
	cmd.Flags().StringVarP(&sinks, "sinks", "s", "", "List of sink types")
	cmd.Flags().StringVarP(&processors, "processors", "p", "", "List of processor types")

	if err := cmd.MarkFlagRequired("scope"); err != nil {
		panic(err)
	}

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

	qs := []*survey.Question{
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

	qs := []*survey.Question{
		{
			Name: "processor",
			Prompt: &survey.MultiSelect{
				Message: "Select processor",
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

	qs := []*survey.Question{
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
