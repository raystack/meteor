package processors

func PopulateStore(store *Store) {
	store.Populate(map[string]Processor{
		"metadata": new(AddMetadataProcessor),
	})
}
