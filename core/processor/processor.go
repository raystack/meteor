package processor

import (
	"fmt"
)

type Model interface {
	GetMetadataField()
	SetMetadataField()
}

type Dataset struct{}
type Job struct{}

type Processor interface {
	Process(data []map[string]interface{}, config map[string]interface{}) ([]map[string]interface{}, error)
}

type NotFoundError struct {
	Name string
}

func (err NotFoundError) Error() string {
	return fmt.Sprintf("could not find processor \"%s\"", err.Name)
}
