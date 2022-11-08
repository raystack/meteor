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

	s.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
	s.SetMaxAllocs(maxAllocs)
	s.SetMaxConstObjects(maxConsts)

	return s
}
