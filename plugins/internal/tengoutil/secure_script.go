package tengoutil

import (
	"github.com/d5/tengo/v2"
	"github.com/d5/tengo/v2/stdlib"
)

const (
	maxAllocs = 5000
	maxConsts = 500
)

func NewSecureScript(input []byte) *tengo.Script {
	s := tengo.NewScript(input)

	s.SetImports(stdlib.GetModuleMap(
		// `os` is excluded, should not be importable from script.
		"math", "text", "times", "rand", "fmt", "json", "base64", "hex", "enum",
	))
	s.SetMaxAllocs(maxAllocs)
	s.SetMaxConstObjects(maxConsts)

	return s
}
