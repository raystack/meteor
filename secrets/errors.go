package secrets

import "fmt"

type NotFoundError struct {
	SecretName string
}

func (err NotFoundError) Error() string {
	return fmt.Sprintf("could not find secret with name \"%s\"", err.SecretName)
}
