package registry

import (
	"github.com/goto/meteor/plugins"
	"github.com/pkg/errors"
)

// ExtractorFactory is a factory for Extractors.
type ExtractorFactory struct {
	fnStore map[string]func() plugins.Extractor
}

// Get returns an Extractor by name.
func (f *ExtractorFactory) Get(name string) (plugins.Extractor, error) {
	if fn, ok := f.fnStore[name]; ok {
		return fn(), nil
	}
	return nil, plugins.NotFoundError{Type: plugins.PluginTypeExtractor, Name: name}
}

// Info returns information about an Extractor.
func (f *ExtractorFactory) Info(name string) (plugins.Info, error) {
	sink, err := f.Get(name)
	if err != nil {
		return plugins.Info{}, err
	}
	return sink.Info(), nil
}

// List returns a list of registered Extractors.
func (f *ExtractorFactory) List() map[string]plugins.Info {
	list := make(map[string]plugins.Info)
	for name := range f.fnStore {
		info, _ := f.Info(name)
		list[name] = info
	}
	return list
}

// Register registers an Extractor.
func (f *ExtractorFactory) Register(name string, extractorFn func() plugins.Extractor) (err error) {
	if _, ok := f.fnStore[name]; ok {
		return errors.Errorf("duplicate extractor: %s", name)
	}
	f.fnStore[name] = extractorFn
	return nil
}
