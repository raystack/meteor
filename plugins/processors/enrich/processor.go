package enrich

import (
	"errors"

	"github.com/odpf/meteor/core/processor"
	"github.com/odpf/meteor/plugins/utils"
)

type Processor struct{}

func New() processor.Processor {
	return new(Processor)
}

func (p *Processor) Process(
	data interface{},
	config map[string]interface{},
) (
	result interface{},
	err error,
) {
	result = data

	if config == nil {
		return result, errors.New("invalid config")
	}

	err = utils.ModifyCustomProperties(data, func(custom map[string]string) (err error) {
		for key, value := range config {
			stringVal, ok := value.(string)
			if ok {
				custom[key] = stringVal
			}
		}

		return
	})

	return
}
