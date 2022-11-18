package merlin

import "fmt"

type APIError struct {
	Method   string
	Endpoint string
	Status   int
	Msg      string
}

func (e *APIError) Error() string {
	return fmt.Sprintf(
		"[%s]: %s: unexpected response status '%d': %s",
		e.Method, e.Endpoint, e.Status, e.Msg,
	)
}
