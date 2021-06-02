package sinks

func PopulateStore(store *Store) {
	store.Populate(map[string]Sink{
		"console": new(ConsoleSink),
	})
}
