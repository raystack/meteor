package registry

import (
	"github.com/odpf/meteor/plugins"
	"github.com/pkg/errors"
)

// ExtractorFactory is a factory for Extractors.
type ExtractorFactory struct {
	fnStore map[string]func() plugins.Extractor
}

func (f *ExtractorFactory) Get(name string) (plugins.Extractor, error) {
	if fn, ok := f.fnStore[name]; ok {
		return fn(), nil
	}
	return nil, plugins.NotFoundError{Type: plugins.PluginTypeExtractor, Name: name}
}

func (f *ExtractorFactory) GetInfo(name string) (plugins.Extractor, error) {
	if fn, ok := f.fnStore[name]; ok {
		return fn(), nil
	}
	return nil, plugins.NotFoundError{Type: plugins.PluginTypeExtractor, Name: name}
}

func (f *ExtractorFactory) List() (names [][]string) {

	for name := range f.fnStore {
		names = append(names, []string{name, "extractor"})
	}
	return
}

func (f *ExtractorFactory) Register(name string, extractorFn func() plugins.Extractor) (err error) {
	if _, ok := f.fnStore[name]; ok {
		return errors.Errorf("duplicate extractor: %s", name)
	}
	f.fnStore[name] = extractorFn
	return nil
}
