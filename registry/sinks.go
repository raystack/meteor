package registry

import (
	"github.com/odpf/meteor/plugins"
	"github.com/pkg/errors"
)

// SinkFactory is a factory for Sinks.
type SinkFactory struct {
	fnStore map[string]func() plugins.Syncer
}

func (f *SinkFactory) Get(name string) (plugins.Syncer, error) {
	if fn, ok := f.fnStore[name]; ok {
		return fn(), nil
	}
	return nil, plugins.NotFoundError{Type: plugins.PluginTypeSink, Name: name}
}

// func (f *SinkFactory) GetInfo(name string) (info plugins.PluginInfo, err error) {
// 	path, ok := f.infoStore[name]
// 	if !ok {
// 		return info, plugins.NotFoundError{Type: plugins.PluginTypeSink, Name: name}
// 	}

// 	return buildPluginInfo(path)
// }

func (f *SinkFactory) List() (names [][]string) {

	for name := range f.fnStore {
		names = append(names, []string{name, "sink"})
	}
	return
}

func (f *SinkFactory) Register(name string, fn func() plugins.Syncer) (err error) {
	if _, ok := f.fnStore[name]; ok {
		return errors.Errorf("duplicate syncer: %s", name)
	}

	f.fnStore[name] = fn

	return
}
