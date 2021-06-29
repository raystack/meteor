package recipes

import (
	"fmt"
)

type InvalidRecipeError struct {
	Message string
}

func (err InvalidRecipeError) Error() string {
	return fmt.Sprintf("invalid recipe: \"%s\"", err.Message)
}

type RunTaskError struct {
	Task Task
	Err  error
}

func (e RunTaskError) Error() string {
	return fmt.Sprintf(
		"error running %s task \"%s\": %s",
		e.Task.Type,
		e.Task.Name,
		e.Err)
}
