package extractor

import (
	"github.com/odpf/meteor/core"
	"github.com/pkg/errors"
)

var (
	Catalog = NewFactory()
)

type Factory struct {
	fnStore map[string]func() core.Extractor
}

func NewFactory() *Factory {
	return &Factory{
		fnStore: map[string]func() core.Extractor{},
	}
}

func (f *Factory) Get(name string) (core.Extractor, error) {
	if fn, ok := f.fnStore[name]; ok {
		return fn(), nil
	}
	return nil, NotFoundError{name}
}

func (f *Factory) Register(name string, extractorFn func() core.Extractor) (err error) {
	if _, ok := f.fnStore[name]; ok {
		return errors.Errorf("duplicate extractor: %s", name)
	}
	f.fnStore[name] = extractorFn
	return nil
}
