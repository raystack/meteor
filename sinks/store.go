package sinks

type Store struct {
	sinks map[string]Sink
}

func NewStore() *Store {
	return &Store{
		sinks: make(map[string]Sink),
	}
}

func (s *Store) Populate(sinks map[string]Sink) {
	for name, ext := range sinks {
		s.sinks[name] = ext
	}
}

func (s *Store) Find(name string) (Sink, error) {
	sink, ok := s.sinks[name]
	if !ok {
		return nil, NotFoundError{name}
	}

	return sink, nil
}
