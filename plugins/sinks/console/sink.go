package console

import (
	"encoding/json"
	"fmt"

	"github.com/odpf/meteor/core/sink"
)

type Sink struct{}

func New() sink.Sink {
	return new(Sink)
}

func (sink *Sink) Sink(data interface{}, config map[string]interface{}) (err error) {
	jsonBytes, err := json.Marshal(data)
	fmt.Println(string(jsonBytes))

	return err
}
