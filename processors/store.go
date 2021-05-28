package processors

type Store struct {
	processors map[string]Processor
}

func NewStore() *Store {
	return &Store{
		processors: make(map[string]Processor),
	}
}

func (s *Store) Populate(processors map[string]Processor) {
	for name, proc := range processors {
		s.processors[name] = proc
	}
}

func (s *Store) Find(name string) (Processor, error) {
	processor, ok := s.processors[name]
	if !ok {
		return nil, NotFoundError{name}
	}

	return processor, nil
}
