package extractors

func PopulateStore(store *Store) {
	store.Populate(map[string]Extractor{
		"kafka": new(KafkaExtractor),
	})
}
