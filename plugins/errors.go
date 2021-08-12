package plugins

import "fmt"

type InvalidConfigError struct {
}

func (err InvalidConfigError) Error() string {
	return "invalid extractor config"
}

type NotFoundError struct {
	Type string
	Name string
}

func (err NotFoundError) Error() string {
	return fmt.Sprintf("could not find %s \"%s\"", err.Type, err.Name)
}
