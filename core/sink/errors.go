package sink

import "fmt"

type NotFoundError struct {
	Name string
}

func (err NotFoundError) Error() string {
	return fmt.Sprintf("could not find sink \"%s\"", err.Name)
}
