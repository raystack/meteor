package registry

import (
	"github.com/odpf/meteor/plugins"
	"github.com/pkg/errors"
)

// ProcessorFactory is a factory for Processors.
type ProcessorFactory struct {
	fnStore map[string]func() plugins.Processor
}

func (f *ProcessorFactory) Get(name string) (plugins.Processor, error) {
	if fn, ok := f.fnStore[name]; ok {
		return fn(), nil
	}
	return nil, plugins.NotFoundError{Type: plugins.PluginTypeProcessor, Name: name}
}

func (f *ProcessorFactory) List() (names [][]string) {

	for name := range f.fnStore {
		names = append(names, []string{name, "processor"})
	}
	return
}

func (f *ProcessorFactory) Register(name string, fn func() plugins.Processor) (err error) {
	if _, ok := f.fnStore[name]; ok {
		return errors.Errorf("duplicate processor: %s", name)
	}
	f.fnStore[name] = fn
	return nil
}
