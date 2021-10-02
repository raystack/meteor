package generator

import (
	"embed"
	"os"
	"strings"
	"text/template"

	"github.com/odpf/meteor/registry"
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

var genericMap = map[string]interface{}{
	"indent":  indent,
	"nindent": nindent,
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
		tem.Source[source] = sinfo.SampleConfig
	}
	if len(sinks) > 0 {
		tem.Sinks = make(map[string]string)
		for _, sink := range sinks {
			info, err := registry.Sinks.Info(sink)
			if err != nil {
				return err
			}
			tem.Sinks[sink] = info.SampleConfig
		}
	}

	if len(processors) > 0 {
		tem.Processors = make(map[string]string)
		for _, procc := range processors {
			info, err := registry.Processors.Info(procc)
			if err != nil {
				return err
			}
			tem.Processors[procc] = info.SampleConfig
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

func indent(spaces int, v string) string {
	pad := strings.Repeat(" ", spaces)
	return pad + strings.Replace(v, "\n", "\n"+pad, -1)
}

func nindent(spaces int, v string) string {
	return "\n" + indent(spaces, v)
}
