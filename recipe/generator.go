package recipe

import (
	"fmt"
	"os"
	"path"
	"text/template"
)

type FromTemplateData struct {
	FileName string                 `json:"FileName" yaml:"FileName"`
	Data     map[string]interface{} `json:"Data" yaml:"Data"`
}

type FromTemplateConfig struct {
	TemplateFilePath string
	OutputDirPath    string
	DataPath         string
	Data             []FromTemplateData
}

func FromTemplate(config FromTemplateConfig) error {
	tmplt, err := template.ParseFiles(config.TemplateFilePath)
	if err != nil {
		return fmt.Errorf("error parsing template: %w", err)
	}

	err = os.MkdirAll(config.OutputDirPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("error creating output directory: %w", err)
	}

	for _, d := range config.Data {
		if err := outputRecipe(tmplt, config.OutputDirPath, d); err != nil {
			return fmt.Errorf("error executing template: %w", err)
		}
	}

	return nil
}

func outputRecipe(tmplt *template.Template, outputDir string, data FromTemplateData) error {
	file, err := os.Create(path.Join(outputDir, data.FileName+".yaml"))
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}
	defer file.Close()

	if err := tmplt.Execute(file, data); err != nil {
		return fmt.Errorf("error executing template: %w", err)
	}

	return nil
}
