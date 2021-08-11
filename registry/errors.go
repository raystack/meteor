package registry

import "fmt"

type NotFoundError struct {
	Type string
	Name string
}

func (err NotFoundError) Error() string {
	return fmt.Sprintf("could not find %s \"%s\"", err.Type, err.Name)
}
