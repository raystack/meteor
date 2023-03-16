package registry

import (
	"github.com/goto/meteor/plugins"
	"github.com/pkg/errors"
)

// SinkFactory is a factory for Sinks.
type SinkFactory struct {
	fnStore map[string]func() plugins.Syncer
}

// Get returns a Sink by name.
func (f *SinkFactory) Get(name string) (plugins.Syncer, error) {
	if fn, ok := f.fnStore[name]; ok {
		return fn(), nil
	}
	return nil, plugins.NotFoundError{Type: plugins.PluginTypeSink, Name: name}
}

// Info returns information about a Sink.
func (f *SinkFactory) Info(name string) (info plugins.Info, err error) {
	sink, err := f.Get(name)
	if err != nil {
		return plugins.Info{}, err
	}
	return sink.Info(), nil
}

// List returns a list of registered Sinks.
func (f *SinkFactory) List() map[string]plugins.Info {
	list := make(map[string]plugins.Info)
	for name := range f.fnStore {
		info, _ := f.Info(name)
		list[name] = info
	}
	return list
}

// Register registers a Sink.
func (f *SinkFactory) Register(name string, fn func() plugins.Syncer) (err error) {
	if _, ok := f.fnStore[name]; ok {
		return errors.Errorf("duplicate syncer: %s", name)
	}
	f.fnStore[name] = fn
	return
}
