package cmd

import (
	"fmt"
	"os"

	"github.com/MakeNowJust/heredoc"
	"github.com/goto/meteor/recipe"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

// GenCmd creates a command object for the "gen" action
func GenCmd() *cobra.Command {
	var (
		outputDirPath string
		dataFilePath  string
	)

	cmd := &cobra.Command{
		Use:   "gen",
		Args:  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		Short: "Generate recipes",
		Long: heredoc.Doc(`
			Generate multiple recipes using a template and list of data.

			The generated recipes will be created on output directory.`,
		),
		Example: heredoc.Doc(`
			# generate multiple recipes with a template
			$ meteor gen my-template.yaml -o ./output-dir -d ./data.yaml
		`),
		Annotations: map[string]string{
			"group:core": "true",
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
	cmd.Flags().StringVarP(&dataFilePath, "data", "d", "", "Template's data")

	return cmd
}
