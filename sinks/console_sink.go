package sinks

import (
	"encoding/json"
	"fmt"
)

type ConsoleSink struct{}

func (sink *ConsoleSink) Sink(data []map[string]interface{}, config map[string]interface{}) (err error) {
	jsonBytes, err := json.Marshal(data)
	fmt.Println(string(jsonBytes))

	return err
}
