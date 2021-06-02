package extractors

type Store struct {
	extractors map[string]Extractor
}

func NewStore() *Store {
	return &Store{
		extractors: make(map[string]Extractor),
	}
}

func (s *Store) Populate(extractors map[string]Extractor) {
	for name, ext := range extractors {
		s.extractors[name] = ext
	}
}

func (s *Store) Find(name string) (Extractor, error) {
	extractor, ok := s.extractors[name]
	if !ok {
		return nil, NotFoundError{name}
	}

	return extractor, nil
}
