package generator

import (
	_ "embed"
	"fmt"
	"io"
	"strings"
	"text/template"

	"github.com/goto/meteor/registry"
)

//go:embed recipe.yaml
var RecipeTemplate string

// TemplateData represents the template for generating a recipe.
type TemplateData struct {
	Name    string
	Version string
	Source  struct {
		Name         string
		Scope        string
		SampleConfig string
	}
	Sinks      map[string]string
	Processors map[string]string
}

var TemplateFuncs = map[string]interface{}{
	"indent": indent,
	"rawfmt": rawfmt,
}

var recipeVersions = [1]string{"v1beta1"}

type RecipeParams struct {
	Name       string
	Source     string
	Scope      string
	Sinks      []string
	Processors []string
}

// Recipe checks if the recipe is valid and returns a Template
func Recipe(p RecipeParams) (*TemplateData, error) {
	tem := &TemplateData{
		Name:    p.Name,
		Version: recipeVersions[len(recipeVersions)-1],
	}

	if p.Source != "" {
		tem.Source.Name = p.Source
		tem.Source.Scope = p.Scope

		sourceInfo, err := registry.Extractors.Info(p.Source)
		if err != nil {
			return nil, fmt.Errorf("provide extractor information: %w", err)
		}

		tem.Source.SampleConfig = sourceInfo.SampleConfig
	}
	if len(p.Sinks) > 0 {
		tem.Sinks = make(map[string]string)
		for _, sink := range p.Sinks {
			info, err := registry.Sinks.Info(sink)
			if err != nil {
				return nil, fmt.Errorf("provide sink information: %w", err)
			}
			tem.Sinks[sink] = info.SampleConfig
		}
	}
	if len(p.Processors) > 0 {
		tem.Processors = make(map[string]string)
		for _, procc := range p.Processors {
			info, err := registry.Processors.Info(procc)
			if err != nil {
				return nil, fmt.Errorf("provide processor information: %w", err)
			}
			tem.Processors[procc] = info.SampleConfig
		}
	}

	return tem, nil
}

// RecipeWriteTo build and apply recipe to provided io writer
func RecipeWriteTo(p RecipeParams, writer io.Writer) error {
	tem, err := Recipe(p)
	if err != nil {
		return err
	}
	tmpl := template.Must(
		template.New("recipe.yaml").Funcs(TemplateFuncs).Parse(RecipeTemplate),
	)
	if err := tmpl.Execute(writer, *tem); err != nil {
		return fmt.Errorf("execute template: %w", err)
	}
	return nil
}

func indent(spaces int, v string) string {
	pad := strings.Repeat(" ", spaces)
	return pad + strings.Replace(v, "\n", "\n"+pad, -1)
}

func rawfmt(s string) string {
	if !strings.HasPrefix(s, "\n") {
		s = "\n" + s
	}

	return strings.ReplaceAll(s, "\t", "  ")
}

func GetRecipeVersions() [1]string {
	return recipeVersions
}
