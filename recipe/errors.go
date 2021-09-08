package recipe

import (
	"fmt"
)

// InvalidRecipeError hold the field to show the error message
type InvalidRecipeError struct {
	Message string
}

func (err InvalidRecipeError) Error() string {
	return fmt.Sprintf("invalid recipe: \"%s\"", err.Message)
}
