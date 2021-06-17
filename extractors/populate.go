package extractors

import "github.com/odpf/meteor/extractors/bigquerydataset"

func PopulateStore(store *Store) {
	store.Populate(map[string]Extractor{
		"kafka":           new(KafkaExtractor),
		"bigquerydataset": new(bigquerydataset.Extractor),
	})
}
