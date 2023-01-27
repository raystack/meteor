package tengoutil

import (
	"fmt"

	"github.com/d5/tengo/v2"
	"github.com/d5/tengo/v2/stdlib"
)

const (
	maxAllocs = 5000
	maxConsts = 500
)

func NewSecureScript(input []byte, globals map[string]interface{}) (*tengo.Script, error) {
	s := tengo.NewScript(input)

	s.SetImports(stdlib.GetModuleMap(
		// `os` is excluded, should *not* be importable from script.
		"math", "text", "times", "rand", "fmt", "json", "base64", "hex", "enum",
	))
	s.SetMaxAllocs(maxAllocs)
	s.SetMaxConstObjects(maxConsts)

	for name, v := range globals {
		if err := s.Add(name, v); err != nil {
			return nil, fmt.Errorf("new secure script: declare globals: %w", err)
		}
	}

	return s, nil
}
