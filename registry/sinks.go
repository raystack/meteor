package registry

import (
	"strings"

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

func (f *SinkFactory) Info(name string) (info plugins.Info, err error) {
	sink, err := f.Get(name)
	if err != nil {
		return plugins.Info{}, err
	}
	return sink.Info(), nil
}

func (f *SinkFactory) List() (names [][]string) {
	for name := range f.fnStore {
		info, _ := f.Info(name)
		names = append(names, []string{name, info.Description, strings.Join(info.Tags, ",")})
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
