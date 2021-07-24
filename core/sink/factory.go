package sink

import (
	"fmt"
	"github.com/odpf/meteor/core"
	"github.com/pkg/errors"
)

var (
	Catalog = NewFactory()
)

type Factory struct {
	fnStore map[string]core.Syncer
}

func NewFactory() *Factory {
	return &Factory{
		fnStore: make(map[string]core.Syncer),
	}
}

func (f *Factory) Get(name string) (core.Syncer, error) {
	if sync, ok := f.fnStore[name]; ok {
		return sync, nil
	}
	return nil, NotFoundError{name}
}

func (f *Factory) Register(name string, syncer core.Syncer) (err error) {
	if _, ok := f.fnStore[name]; ok {
		return errors.Errorf("duplicate syncer: %s", name)
	}
	f.fnStore[name] = syncer
	return nil
}

type NotFoundError struct {
	Name string
}

func (err NotFoundError) Error() string {
	return fmt.Sprintf("could not find sink \"%s\"", err.Name)
}
