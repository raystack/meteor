package generator

import (
	"embed"
	"os"
	"strings"
	"text/template"

	"github.com/odpf/meteor/registry"
	"github.com/pkg/errors"
)

//go:embed recipe.yaml
var file embed.FS

// templateData represents the template for generating a recipe.
type templateData struct {
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

var templateFuncs = map[string]interface{}{
	"indent":          indent,
	"fmtSampleConfig": fmtSampleConfig,
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
func Recipe(p RecipeParams) error {
	tem := templateData{
		Name:    p.Name,
		Version: recipeVersions[len(recipeVersions)-1],
	}

	if p.Source != "" {
		tem.Source.Name = p.Source
		tem.Source.Scope = p.Scope

		sourceInfo, err := registry.Extractors.Info(p.Source)
		if err != nil {
			return errors.Wrap(err, "failed to provide extractor information")
		}

		tem.Source.SampleConfig = sourceInfo.SampleConfig
	}
	if len(p.Sinks) > 0 {
		tem.Sinks = make(map[string]string)
		for _, sink := range p.Sinks {
			info, err := registry.Sinks.Info(sink)
			if err != nil {
				return errors.Wrap(err, "failed to provide sink information")
			}
			tem.Sinks[sink] = info.SampleConfig
		}
	}
	if len(p.Processors) > 0 {
		tem.Processors = make(map[string]string)
		for _, procc := range p.Processors {
			info, err := registry.Processors.Info(procc)
			if err != nil {
				return errors.Wrap(err, "failed to provide processor information")
			}
			tem.Processors[procc] = info.SampleConfig
		}
	}

	tmpl := template.Must(
		template.New("recipe.yaml").Funcs(templateFuncs).ParseFS(file, "*"),
	)
	if err := tmpl.Execute(os.Stdout, tem); err != nil {
		return errors.Wrap(err, "failed to execute template")
	}
	return nil
}

func indent(spaces int, v string) string {
	pad := strings.Repeat(" ", spaces)
	return pad + strings.Replace(v, "\n", "\n"+pad, -1)
}

func fmtSampleConfig(s string) string {
	if !strings.HasPrefix(s, "\n") {
		s = "\n" + s
	}

	return strings.ReplaceAll(s, "\t", "  ")
}

func GetRecipeVersions() [1]string {
	return recipeVersions
}
