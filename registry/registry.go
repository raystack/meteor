package registry

import (
	"github.com/odpf/meteor/plugins"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

var (
	Extractors = NewExtractorFactory()
	Sinks      = NewSinkFactory()
	Processors = NewProcessorFactory()
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

// SinkFactory is a factory for Sinks.
type SinkFactory struct {
	fnStore   map[string]func() plugins.Syncer
	infoStore map[string]string // map[plugin name]plugin info content
}

func (f *SinkFactory) Get(name string) (plugins.Syncer, error) {
	if fn, ok := f.fnStore[name]; ok {
		return fn(), nil
	}
	return nil, plugins.NotFoundError{Type: plugins.PluginTypeSink, Name: name}
}

func (f *SinkFactory) GetInfo(name string) (info plugins.PluginInfo, err error) {
	path, ok := f.infoStore[name]
	if !ok {
		return info, plugins.NotFoundError{Type: plugins.PluginTypeSink, Name: name}
	}

	return buildPluginInfo(path)
}

func (f *SinkFactory) List() (names [][]string) {

	for name := range f.fnStore {
		names = append(names, []string{name, "sink"})
	}
	return
}

func (f *SinkFactory) Register(name string, fn func() plugins.Syncer, pluginInfo string) (err error) {
	if _, ok := f.fnStore[name]; ok {
		return errors.Errorf("duplicate syncer: %s", name)
	}

	f.fnStore[name] = fn
	f.infoStore[name] = pluginInfo

	return
}

func buildPluginInfo(pluginInfoString string) (info plugins.PluginInfo, err error) {
	err = yaml.Unmarshal([]byte(pluginInfoString), &info)
	if err != nil {
		return
	}

	return
}

func NewExtractorFactory() *ExtractorFactory {
	return &ExtractorFactory{
		fnStore: map[string]func() plugins.Extractor{},
	}
}

func NewProcessorFactory() *ProcessorFactory {
	return &ProcessorFactory{
		fnStore: make(map[string]func() plugins.Processor),
	}
}

func NewSinkFactory() *SinkFactory {
	return &SinkFactory{
		fnStore:   make(map[string]func() plugins.Syncer),
		infoStore: make(map[string]string),
	}
}
