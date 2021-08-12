package enrich

import (
	"context"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
)

type Processor struct{}

func New() *Processor {
	return new(Processor)
}

func (p *Processor) Process(ctx context.Context, config map[string]interface{}, in <-chan interface{}, out chan<- interface{}) (err error) {
	for data := range in {
		out <- p.process(data, config)
	}

	return
}

func (p *Processor) process(data interface{}, config map[string]interface{}) interface{} {
	customProps := utils.GetCustomProperties(data)
	if customProps == nil {
		return data
	}

	// update custom properties using value from config
	for key, value := range config {
		stringVal, ok := value.(string)
		if ok {
			customProps[key] = stringVal
		}
	}

	// save custom properties
	result := utils.SetCustomProperties(data, customProps)
	return result
}

func init() {
	registry.Processors.Register("enrich", func() plugins.Processor {
		return New()
	})
}
