package extractors

type Store struct {
	extractors map[string]Extractor
}

func NewStore() *Store {
	return &Store{
		extractors: make(map[string]Extractor),
	}
}

func (s *Store) Get(name string) (Extractor, error) {
	extractor, ok := s.extractors[name]
	if !ok {
		return nil, NotFoundError{name}
	}

	return extractor, nil
}

func (s *Store) Set(name string, extractor Extractor) {
	s.extractors[name] = extractor
}
