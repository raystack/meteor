package cmd

import (
	"io/ioutil"

	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/meteor/recipe"
	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

// GenCmd creates a command object for the "gen" action
func GenCmd(lg log.Logger) *cobra.Command {
	var (
		outputDirPath string
		dataFilePath  string
	)

	cmd := &cobra.Command{
		Use:   "gen",
		Args:  cobra.ExactValidArgs(1),
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
			bytes, err := ioutil.ReadFile(dataFilePath)
			if err != nil {
				return err
			}

			var data []recipe.FromTemplateData
			err = yaml.Unmarshal(bytes, &data)
			if err != nil {
				return err
			}

			return recipe.FromTemplate(recipe.FromTemplateConfig{
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
