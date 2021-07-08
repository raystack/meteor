package processors

import (
	"github.com/odpf/meteor/pkg/processors/enrich"
	"github.com/odpf/meteor/processors"
)

func PopulateStore(store *processors.Store) {
	store.Set("enrich", new(enrich.Processor))
}
