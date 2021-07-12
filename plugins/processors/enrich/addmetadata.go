package enrich

import (
	"errors"

	"github.com/odpf/meteor/core/processor"
)

type Processor struct{}

func New() processor.Processor {
	return new(Processor)
}

func (p *Processor) Process(
	data []map[string]interface{},
	config map[string]interface{},
) (
	result []map[string]interface{},
	err error,
) {
	if config == nil {
		return result, errors.New("invalid config")
	}

	for _, d := range data {
		p.appendFields(d, config)
	}

	return data, nil
}

func (p *Processor) appendFields(d map[string]interface{}, metadata map[string]interface{}) {
	for key, value := range metadata {
		d[key] = value
	}
}
