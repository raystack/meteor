package plugins

import "fmt"

type InvalidConfigError struct {
	Type PluginType
}

func (err InvalidConfigError) Error() string {
	return fmt.Sprintf("invalid %s config", err.Type)
}

type NotFoundError struct {
	Type PluginType
	Name string
}

func (err NotFoundError) Error() string {
	return fmt.Sprintf("could not find %s \"%s\"", err.Type, err.Name)
}
