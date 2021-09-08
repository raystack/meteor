package plugins

import "fmt"

// InvalidConfigError is returned when a plugin's configuration is invalid.
type InvalidConfigError struct {
	Type PluginType
}

func (err InvalidConfigError) Error() string {
	return fmt.Sprintf("invalid %s config", err.Type)
}

// NotFoundError contains fields required to checks for a missing plugin.
type NotFoundError struct {
	Type PluginType
	Name string
}

func (err NotFoundError) Error() string {
	return fmt.Sprintf("could not find %s \"%s\"", err.Type, err.Name)
}
