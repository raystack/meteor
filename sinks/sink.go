package sinks

import "fmt"

type Sink interface {
	Sink(data []map[string]interface{}, config map[string]interface{}) error
}

type NotFoundError struct {
	Name string
}

func (err NotFoundError) Error() string {
	return fmt.Sprintf("could not find sink \"%s\"", err.Name)
}
