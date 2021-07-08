package sinks

type Store struct {
	sinks map[string]Sink
}

func NewStore() *Store {
	return &Store{
		sinks: make(map[string]Sink),
	}
}

func (s *Store) Get(name string) (Sink, error) {
	sink, ok := s.sinks[name]
	if !ok {
		return nil, NotFoundError{name}
	}

	return sink, nil
}

func (s *Store) Set(name string, sink Sink) {
	s.sinks[name] = sink
}
