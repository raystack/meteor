package domain

import (
	"errors"
	"fmt"
)

var (
	ErrDuplicateRecipeName = errors.New("recipe name already exists")
)

type RecipeNotFoundError struct {
	RecipeName string
}

func (err RecipeNotFoundError) Error() string {
	return fmt.Sprintf("could not find recipe with name \"%s\"", err.RecipeName)
}

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
