package console

import (
	"encoding/json"
	"fmt"

	"github.com/odpf/meteor/sinks"
)

type Sink struct{}

func New() sinks.Sink {
	return new(Sink)
}

func (sink *Sink) Sink(data []map[string]interface{}, config map[string]interface{}) (err error) {
	jsonBytes, err := json.Marshal(data)
	fmt.Println(string(jsonBytes))

	return err
}
