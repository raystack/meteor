package extractors

import (
	"github.com/odpf/meteor/extractors/bigquerydataset"
	"github.com/odpf/meteor/extractors/bigquerytable"
)

func PopulateStore(store *Store) {
	store.Populate(map[string]Extractor{
		"kafka":           new(KafkaExtractor),
		"bigquerydataset": new(bigquerydataset.Extractor),
		"bigquerytable":   new(bigquerytable.Extractor),
	})
}
