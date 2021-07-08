package console

import (
	"encoding/json"
	"fmt"
)

type Sink struct{}

func (sink *Sink) Sink(data []map[string]interface{}, config map[string]interface{}) (err error) {
	jsonBytes, err := json.Marshal(data)
	fmt.Println(string(jsonBytes))

	return err
}
