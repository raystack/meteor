package extractor

import (
	"errors"
)

type Extractor struct {
	factory *Factory
}

func New(factory *Factory) *Extractor {
	return &Extractor{factory: factory}
}

func (e *Extractor) Extract(name string, config map[string]interface{}) (data interface{}, err error) {
	extr, err := e.factory.Get(name)
	if err != nil {
		return
	}

	switch extr := extr.(type) {
	case TableExtractor:
		data, err = extr.Extract(config)
	case TopicExtractor:
		data, err = extr.Extract(config)
	case DashboardExtractor:
		data, err = extr.Extract(config)
	case UserExtractor:
		data, err = extr.Extract(config)
	case BucketExtractor:
		data, err = extr.Extract(config)
	default:
		err = errors.New("invalid extractor type")
	}

	return
}
