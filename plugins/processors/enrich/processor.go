package enrich

import (
	"github.com/odpf/meteor/utils"
)

type Processor struct{}

func New() *Processor {
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
