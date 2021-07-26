package sink

import (
	"github.com/odpf/meteor/core"
	"github.com/pkg/errors"
)

var (
	Catalog = NewFactory()
)

type Factory struct {
	fnStore map[string]func() core.Syncer
}

func NewFactory() *Factory {
	return &Factory{
		fnStore: make(map[string]func() core.Syncer),
	}
}

func (f *Factory) Get(name string) (core.Syncer, error) {
	if fn, ok := f.fnStore[name]; ok {
		return fn(), nil
	}
	return nil, NotFoundError{name}
}

func (f *Factory) Register(name string, fn func() core.Syncer) (err error) {
	if _, ok := f.fnStore[name]; ok {
		return errors.Errorf("duplicate syncer: %s", name)
	}

	f.fnStore[name] = fn

	return
}
