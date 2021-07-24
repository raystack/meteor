package processor

import (
	"fmt"
	"github.com/odpf/meteor/core"
	"github.com/pkg/errors"
)

var (
	Catalog = NewFactory()
)

type Factory struct {
	fnStore map[string]core.Processor
}

func NewFactory() *Factory {
	return &Factory{
		fnStore: make(map[string]core.Processor),
	}
}

func (f *Factory) Get(name string) (core.Processor, error) {
	if processor, ok := f.fnStore[name]; ok {
		return processor, nil
	}
	return nil, NotFoundError{name}
}

func (f *Factory) Register(name string, processor core.Processor) (err error) {
	if _, ok := f.fnStore[name]; ok {
		return errors.Errorf("duplicate processor: %s", name)
	}
	f.fnStore[name] = processor
	return nil
}


type NotFoundError struct {
	Name string
}

func (err NotFoundError) Error() string {
	return fmt.Sprintf("could not find processor \"%s\"", err.Name)
}
