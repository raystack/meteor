package processors

type Store struct {
	processors map[string]Processor
}

func NewStore() *Store {
	return &Store{
		processors: make(map[string]Processor),
	}
}

func (s *Store) Get(name string) (Processor, error) {
	processor, ok := s.processors[name]
	if !ok {
		return nil, NotFoundError{name}
	}

	return processor, nil
}

func (s *Store) Set(name string, processor Processor) {
	s.processors[name] = processor
}
