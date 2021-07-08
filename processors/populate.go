package processors

func PopulateStore(store *Store) {
	store.Set("metadata", new(AddMetadataProcessor))
}
