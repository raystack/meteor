package registry

import (
	"github.com/goto/meteor/plugins"
	"github.com/pkg/errors"
)

// ProcessorFactory is a factory for Processors.
type ProcessorFactory struct {
	fnStore map[string]func() plugins.Processor
}

// Get returns a Processor by name.
func (f *ProcessorFactory) Get(name string) (plugins.Processor, error) {
	if fn, ok := f.fnStore[name]; ok {
		return fn(), nil
	}
	return nil, plugins.NotFoundError{Type: plugins.PluginTypeProcessor, Name: name}
}

// Info returns information about a Processor.
func (f *ProcessorFactory) Info(name string) (info plugins.Info, err error) {
	sink, err := f.Get(name)
	if err != nil {
		return plugins.Info{}, err
	}
	return sink.Info(), nil
}

// List returns a list of registered processors.
func (f *ProcessorFactory) List() map[string]plugins.Info {
	list := make(map[string]plugins.Info)
	for name := range f.fnStore {
		info, _ := f.Info(name)
		list[name] = info
	}
	return list
}

// Register registers a Processor.
func (f *ProcessorFactory) Register(name string, fn func() plugins.Processor) (err error) {
	if _, ok := f.fnStore[name]; ok {
		return errors.Errorf("duplicate processor: %s", name)
	}
	f.fnStore[name] = fn
	return nil
}
