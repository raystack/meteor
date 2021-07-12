package processors

import (
	"github.com/odpf/meteor/core/processor"
	"github.com/odpf/meteor/plugins/processors/enrich"
)

func PopulateFactory(factory *processor.Factory) {
	factory.Set("enrich", enrich.New)
}
