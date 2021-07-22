package extractor

import (
	"fmt"
	"github.com/odpf/meteor/core"
	"github.com/pkg/errors"
)

var (
	Catalog = NewFactory()
)

type Factory struct {
	fnStore          map[string]core.Extractor
}

func NewFactory() *Factory {
	return &Factory{
		fnStore: map[string]core.Extractor{},
	}
}

func (f *Factory) Get(name string) (core.Extractor, error) {
	if extractor, ok := f.fnStore[name]; ok {
		return extractor, nil
	}
	return nil, NotFoundError{name}
}

func (f *Factory) Register(name string, extractor core.Extractor) (err error) {
	if _, ok := f.fnStore[name]; ok {
		return errors.Errorf("duplicate extractor: %s", name)
	}
	f.fnStore[name] = extractor
	return nil
}

type NotFoundError struct {
	Name string
}

func (err NotFoundError) Error() string {
	return fmt.Sprintf("could not find extractor \"%s\"", err.Name)
}