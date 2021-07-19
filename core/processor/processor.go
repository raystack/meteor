package processor

import (
	"fmt"
)

type Processor interface {
	Process(data interface{}, config map[string]interface{}) (interface{}, error)
}

type NotFoundError struct {
	Name string
}

func (err NotFoundError) Error() string {
	return fmt.Sprintf("could not find processor \"%s\"", err.Name)
}
