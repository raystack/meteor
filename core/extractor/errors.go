package extractor

import "fmt"

type InvalidConfigError struct {
}

func (err InvalidConfigError) Error() string {
	return "invalid extractor config"
}

type NotFoundError struct {
	Name string
}

func (err NotFoundError) Error() string {
	return fmt.Sprintf("could not find extractor \"%s\"", err.Name)
}
