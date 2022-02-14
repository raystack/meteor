package cmd

import (
	"errors"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/meteor/generator"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
)

// NewCmd creates a command object for the "new" action
func NewCmd(lg log.Logger) *cobra.Command {
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
		extractor   string
		sinks       string
		processors  string
		interactive bool
	)

	cmd := &cobra.Command{
		Use:     "recipe [name]",
		Aliases: []string{"r"},
		Args:    cobra.ExactValidArgs(1),
		Short:   "Generate a new recipe",
		Long: heredoc.Doc(`
			Generate a new recipe.

			The recipe will be printed on standard output.
			Specify recipe name with the first argument without extension.
			Use commma to separate multiple sinks and processors.
			Interactive mode can be enabled by setting --interactive or -i to true`),
		Example: heredoc.Doc(`
			# generate a recipe with a bigquery extractor and a console sink
			$ meteor new recipe sample -e bigquery -s console

			# generate recipe with multiple sinks
			$ meteor new recipe sample -e bigquery -s columbus,kafka -p enrich

			# store recipe to a file
			$ meteor new recipe sample -e bigquery -s columbus > recipe.yaml
		`),
		Annotations: map[string]string{
			"group:core": "true",
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			var sinkList []string
			var procList []string
			var err error

			if interactive {
				extractor, err = recipeExtractorSurvey()
				if err != nil {
					return err
				}

				sinkList, err = recipeSinkSurvey()
				if err != nil {
					return err
				}

				procList, err = recipeProcessorSurvey()
				if err != nil {
					return err
				}
			} else {
				if sinks != "" {
					sinkList = strings.Split(sinks, ",")
				}

				if processors != "" {
					procList = strings.Split(processors, ",")
				}
			}
			return generator.Recipe(args[0], extractor, sinkList, procList)
		},
	}

	cmd.Flags().StringVarP(&extractor, "extractor", "e", "", "Type of extractor")
	cmd.Flags().StringVarP(&sinks, "sinks", "s", "", "List of sink types")
	cmd.Flags().StringVarP(&processors, "processors", "p", "", "List of processor types")
	cmd.Flags().BoolVarP(&interactive, "interactive", "i", false, "Enables interactive mode")

	return cmd

}

func recipeSinkSurvey() ([]string, error) {
	var availableSinks []string
	var sinkInput []string

	for sink, _ := range registry.Sinks.List() {
		availableSinks = append(availableSinks, sink)
	}
	if len(availableSinks) == 0 {
		return []string{}, errors.New("no sinks found")
	}

	var qs = []*survey.Question{
		{
			Name: "sink",
			Prompt: &survey.MultiSelect{
				Message: "What is the sink name?",
				Options: availableSinks,
				Help:    "Select the sink(s) for this recipe",
			},
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

	for processor, _ := range registry.Processors.List() {
		availableProcessors = append(availableProcessors, processor)
	}
	if len(availableProcessors) == 0 {
		return []string{}, errors.New("no processors found")
	}

	var qs = []*survey.Question{
		{
			Name: "processor",
			Prompt: &survey.MultiSelect{
				Message: "What is the processor name?",
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

	for extractor, _ := range registry.Extractors.List() {
		availableExtractors = append(availableExtractors, extractor)
	}
	if len(availableExtractors) == 0 {
		return "", errors.New("no extractors found")
	}

	var qs = []*survey.Question{
		{
			Name: "extractor",
			Prompt: &survey.Select{
				Message: "What is the extractor name?",
				Options: availableExtractors,
				Help:    "Select the extractor for this recipe",
			},
		},
	}

	if err := survey.Ask(qs, &extractorInput); err != nil {
		return "", err
	}

	return extractorInput, nil
}
