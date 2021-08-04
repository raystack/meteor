package recipe

import (
	"fmt"
)

type InvalidRecipeError struct {
	Message string
}

func (err InvalidRecipeError) Error() string {
	return fmt.Sprintf("invalid recipe: \"%s\"", err.Message)
}
