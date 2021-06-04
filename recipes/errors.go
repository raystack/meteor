package recipes

import (
	"errors"
	"fmt"

	"github.com/odpf/meteor/domain"
)

var (
	ErrDuplicateRecipeName = errors.New("recipe name already exists")
)

type NotFoundError struct {
	RecipeName string
}

func (err NotFoundError) Error() string {
	return fmt.Sprintf("could not find recipe with name \"%s\"", err.RecipeName)
}

type InvalidRecipeError struct {
	Message string
}

func (err InvalidRecipeError) Error() string {
	return fmt.Sprintf("invalid recipe: \"%s\"", err.Message)
}

type RunTaskError struct {
	task domain.Task
	err  error
}

func (e RunTaskError) Error() string {
	return fmt.Sprintf(
		"error running %s task \"%s\": %s",
		e.task.Type,
		e.task.Name,
		e.err)
}

func newRunTaskError(task domain.Task, err error) RunTaskError {
	return RunTaskError{
		task: task,
		err:  err,
	}
}
