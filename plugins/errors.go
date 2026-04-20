package plugins

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

var ErrEmptyURNScope = errors.New("urn scope is required to generate unique urn")

// ConfigError contains fields to check error
type ConfigError struct {
	Key     string
	Message string
}

// InvalidConfigError is returned when a plugin's configuration is invalid.
type InvalidConfigError struct {
	Type       PluginType
	PluginName string
	Errors     []ConfigError
}

func (err InvalidConfigError) Error() string {
	ss := make([]string, 0, len(err.Errors))
	for _, e := range err.Errors {
		ss = append(ss, e.Message)
	}

	var details string
	if len(ss) != 0 {
		details = ":\n\t * " + strings.Join(ss, "\n\t * ")
	}

	if err.Type == "" {
		return "invalid config" + details
	}
	return fmt.Sprintf("invalid %s config", err.Type) + details
}

func (err InvalidConfigError) HasError() bool {
	return len(err.Errors) > 0
}

// NotFoundError contains fields required to checks for a missing plugin.
type NotFoundError struct {
	Type      PluginType
	Name      string
	Available []string // populated by registry for suggestions
}

func (err NotFoundError) Is(target error) bool {
	t, ok := target.(NotFoundError)
	if !ok {
		return false
	}
	return err.Type == t.Type && err.Name == t.Name
}

func (err NotFoundError) Error() string {
	msg := fmt.Sprintf("could not find %s %q", err.Type, err.Name)

	if suggestions := err.closestMatches(3); len(suggestions) > 0 {
		msg += fmt.Sprintf(". Did you mean: %s?", strings.Join(suggestions, ", "))
	}

	msg += fmt.Sprintf("\n  Run 'meteor plugins list --type %s' to see available plugins.", err.Type)
	return msg
}

func (err NotFoundError) closestMatches(max int) []string {
	if len(err.Available) == 0 {
		return nil
	}

	type scored struct {
		name  string
		score int
	}

	var matches []scored
	for _, name := range err.Available {
		s := stringSimilarity(err.Name, name)
		if s > 0 {
			matches = append(matches, scored{name, s})
		}
	}

	sort.Slice(matches, func(i, j int) bool {
		return matches[i].score > matches[j].score
	})

	var result []string
	for i, m := range matches {
		if i >= max {
			break
		}
		result = append(result, m.name)
	}
	return result
}

// stringSimilarity returns a simple similarity score based on common substrings.
func stringSimilarity(a, b string) int {
	a, b = strings.ToLower(a), strings.ToLower(b)
	if strings.Contains(b, a) || strings.Contains(a, b) {
		return 100
	}

	score := 0
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] == b[i] {
			score += 3
		}
	}
	for _, c := range a {
		if strings.ContainsRune(b, c) {
			score++
		}
	}
	return score
}

// RetryError is an error signalling that retry is needed for the operation.
type RetryError struct {
	Err error
}

func (e RetryError) Error() string {
	return e.Err.Error()
}

func (e RetryError) Unwrap() error {
	return e.Err
}

func NewRetryError(err error) error {
	if err == nil {
		return nil
	}
	return RetryError{Err: err}
}
