package registry

import (
	"github.com/odpf/meteor/plugins"
)

var (
	Extractors = NewExtractorFactory()
	Sinks      = NewSinkFactory()
	Processors = NewProcessorFactory()
)

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
		fnStore: make(map[string]func() plugins.Syncer),
	}
}
