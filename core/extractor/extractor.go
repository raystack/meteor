package extractor

import "fmt"

type Extractor interface {
	Extract(config map[string]interface{}) (data []map[string]interface{}, err error)
}

type NotFoundError struct {
	Name string
}

func (err NotFoundError) Error() string {
	return fmt.Sprintf("could not find extractor \"%s\"", err.Name)
}
