package processors

import (
	"github.com/odpf/meteor/pkg/processors/enrich"
	"github.com/odpf/meteor/processors"
)

func PopulateFactory(factory *processors.Factory) {
	factory.Set("enrich", enrich.New)
}
