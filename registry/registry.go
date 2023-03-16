package registry

import (
	"github.com/goto/meteor/plugins"
)

var (
	// Extractors is a factory for extractors
	Extractors = NewExtractorFactory()
	// Sinks is a factory for sinks
	Sinks = NewSinkFactory()
	// Processors is a factory for processors
	Processors = NewProcessorFactory()
)

// NewExtractorFactory returns a new ExtractorFactory
func NewExtractorFactory() *ExtractorFactory {
	return &ExtractorFactory{
		fnStore: map[string]func() plugins.Extractor{},
	}
}

// NewProcessorFactory returns a new ProcessorFactory
func NewProcessorFactory() *ProcessorFactory {
	return &ProcessorFactory{
		fnStore: make(map[string]func() plugins.Processor),
	}
}

// NewSinkFactory returns a new SinkFactory
func NewSinkFactory() *SinkFactory {
	return &SinkFactory{
		fnStore: make(map[string]func() plugins.Syncer),
	}
}
