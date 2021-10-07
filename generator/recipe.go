package generator

import (
	"embed"
	"github.com/odpf/meteor/registry"
	"github.com/pkg/errors"
	"os"
	"strings"
	"text/template"
)

//go:embed recipe.yaml
var file embed.FS

// Template represents the template for generating a recipe.
type Template struct {
	Name       string
	Source     map[string]string
	Sinks      map[string]string
	Processors map[string]string
}

var templateFuncs = map[string]interface{}{
	"indent": indent,
}

// Recipe checks if the recipe is valid and returns a Template
func Recipe(name string, source string, sinks []string, processors []string) (err error) {
	tem := Template{
		Name: name,
	}

	if source != "" {
		tem.Source = make(map[string]string)
		sourceInfo, err := registry.Extractors.Info(source)
		if err != nil {
			return errors.Wrap(err, "failed to provide extractor information")
		}
		tem.Source[source] = sourceInfo.SampleConfig
	}
	if len(sinks) > 0 {
		tem.Sinks = make(map[string]string)
		for _, sink := range sinks {
			info, err := registry.Sinks.Info(sink)
			if err != nil {
				return errors.Wrap(err, "failed to provide sink information")
			}
			tem.Sinks[sink] = info.SampleConfig
		}
	}
	if len(processors) > 0 {
		tem.Processors = make(map[string]string)
		for _, procc := range processors {
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
	if err = tmpl.Execute(os.Stdout, tem); err != nil {
		return errors.Wrap(err, "failed to execute template")
	}
	return nil
}

func indent(spaces int, v string) string {
	pad := strings.Repeat(" ", spaces)
	return pad + strings.Replace(v, "\n", "\n"+pad, -1)
}
