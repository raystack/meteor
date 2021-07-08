package sinks

func PopulateStore(store *Store) {
	store.Set("console", new(ConsoleSink))
}
