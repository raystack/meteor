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

type Template struct {
	Name       string
	Source     map[string]string
	Sinks      map[string]string
	Processors map[string]string
}

func Recipe(name string, source string, sinks []string, processors []string) error {

	tem := Template{
		Name:       name,
		Source:     make(map[string]string),
		Sinks:      make(map[string]string),
		Processors: make(map[string]string),
	}

	sinfo, _ := registry.Extractors.Info(source)
	tem.Source[source] = indent.String(sinfo.SampleConfig, size)

	for _, sink := range sinks {
		info, _ := registry.Sinks.Info(sink)
		tem.Sinks[sink] = indent.String(info.SampleConfig, size+3)
	}

	for _, procc := range processors {
		info, _ := registry.Processors.Info(procc)
		tem.Processors[procc] = indent.String(info.SampleConfig, size+3)
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
