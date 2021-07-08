package extractors

type FactoryFn func() Extractor

type Factory struct {
	fnStore map[string]FactoryFn
}

func NewFactory() *Factory {
	return &Factory{
		fnStore: make(map[string]FactoryFn),
	}
}

func (f *Factory) Get(name string) (Extractor, error) {
	factoryFn, ok := f.fnStore[name]
	if !ok {
		return nil, NotFoundError{name}
	}

	return factoryFn(), nil
}

func (f *Factory) Set(name string, fn FactoryFn) {
	f.fnStore[name] = fn
}
