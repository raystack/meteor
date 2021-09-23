package generator

import (
	"embed"
	"os"
	"text/template"

	"github.com/muesli/reflow/indent"
	"github.com/odpf/meteor/registry"
)

//go:embed recipe.yaml
var file embed.FS

var size uint = 2

// Template represents the template for generating a recipe.
type Template struct {
	Name       string
	Source     map[string]string
	Sinks      map[string]string
	Processors map[string]string
}

// Recipe checks if the recipe is valid and returns a Template
func Recipe(name string, source string, sinks []string, processors []string) (err error) {
	tem := Template{
		Name: name,
	}

	if source != "" {
		tem.Source = make(map[string]string)
		sinfo, err := registry.Extractors.Info(source)
		if err != nil {
			return err
		}
		tem.Source[source] = indent.String(sinfo.SampleConfig, size)
	}
	if len(sinks) > 0 {
		tem.Sinks = make(map[string]string)
		for _, sink := range sinks {
			info, err := registry.Sinks.Info(sink)
			if err != nil {
				return err
			}
			tem.Sinks[sink] = indent.String(info.SampleConfig, size+3)
		}
	}

	if len(processors) > 0 {
		tem.Processors = make(map[string]string)
		for _, procc := range processors {
			info, err := registry.Processors.Info(procc)
			if err != nil {
				return err
			}
			tem.Processors[procc] = indent.String(info.SampleConfig, size+3)
		}
	}

	tmpl, err := template.ParseFS(file, "*")

	if err != nil {
		return err
	}

	err = tmpl.Execute(os.Stdout, tem)
	if err != nil {
		return err
	}
	return nil
}
